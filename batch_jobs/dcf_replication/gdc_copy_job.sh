#!/bin/bash
set -uxo pipefail

# Comment this out when we move to using just aws creds account
aws configure set aws_access_key_id "$ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY"
echo "aws credentials configured."

if [[ "$DESTINATION_BUCKET" == s3://* ]]; then
  S3_PREFIX="${DESTINATION_BUCKET%/}"
else
  S3_PREFIX="s3://$DESTINATION_BUCKET"
fi

S3_OBJ="$S3_PREFIX/$KEY"

MAX_RETRIES=3
RETRY_DELAY=10
attempt=1
success=false

# --------------------------------------- Helper Functions --------------------------------------- #
putRecord () {
    # PUT REQUEST TO INDEXD TO UPDATE URLS

    # Function variables
    body=$1

    # Extract data from get request response
    did=$(echo "$body" | jq -r '.did')
    rev=$(echo "$body" | jq -r '.rev')
    urls=$(echo "$body" | jq -r '.urls')
    new_urls=()

    # Extract url data. Remove if s3 url exists and replace with the correct url. Keep any other protocol.
    array_length=$(echo "$urls" | jq '. | length')
    new_urls+=("$S3_OBJ")
    for ((i=0; i<array_length; i++)); do
        url=$(echo "$urls" | jq -r ".[$i]")
        if [[ $url != s3://* ]]; then
            echo "found non s3 url"
            echo $url
            new_urls+=("$url")
        fi
    done
    json_payload=$(printf '%s\n' "${new_urls[@]}" | jq -R . | jq -s '{urls: .}')
    echo "json payload created: $json_payload"

    echo "Updating indexd record..."
    curl --request PUT \
      --url "$HOSTNAME/index/$did?rev=$rev" \
      --header 'content-type: application/json' \
      --user $USERNAME:$PASSWORD \
      --max-time 10 \
      --retry 5 \
      --retry-delay 10 \
      --retry-max-time 40 \
      --data "$json_payload"
}

postRecord () {
    # Initialize arrays
    acl=()
    authz=()
    new_urls=()

    # Assign values
    did=$1
    file_name=$2
    size=$3
    file_md5="$4"
    aws_url="$5"
    acl+=("$6")
    authz+=("$7")
    gdc_api="$8"

    new_urls+=("$aws_url")
    new_urls+=("$gdc_api")

    # Check if file_md5 is already JSON or just a string
    if echo "$file_md5" | jq empty 2>/dev/null; then
        # It's valid JSON, use it as-is
        file_md5_json="$file_md5"
    else
        # It's a string, create JSON object
        file_md5_json=$(jq -n --arg md5 "$file_md5" '{md5: $md5}')
    fi

    # Convert arrays to JSON
    new_urls_json=$(printf '%s\n' "${new_urls[@]}" | jq -R . | jq -s .)
    acl=$(printf '%s\n' "${acl[@]}" | jq -R . | jq -s .)
    authz=$(printf '%s\n' "${authz[@]}" | jq -R . | jq -s .)

    # Create final JSON payload
    size=$((size))

    echo $did
    echo $size
    echo $file_name
    echo $file_md5_json
    echo $acl
    echo $authz
    echo $new_urls_json

    json_payload=$(jq -n \
        --arg did "$did" \
        --argjson size "$size" \
        --arg file_name "$file_name" \
        --argjson file_md5 "$file_md5_json" \
        --argjson acl "$acl" \
        --argjson authz "$authz" \
        --argjson new_urls "$new_urls_json" \
        '{
            did: $did,
            file_name: $file_name,
            hashes: $file_md5,
            size: $size,
            urls: $new_urls,
            acl: $acl,
            authz: $authz,
            form: "object"
         }'
    )

   # Create temp file for response
    temp_file=$(mktemp)
    echo "$json_payload" | jq .

    echo "DEBUG: Making request to $HOSTNAME/index/"

    # Increase timeouts and add verbose output
    http_code=$(curl --request POST \
      --url "$HOSTNAME/index/" \
      --user $USERNAME:$PASSWORD \
      --max-time 30 \
      --retry 3 \
      --retry-delay 2 \
      --retry-max-time 20 \
      --header 'content-type: application/json' \
      --data "$json_payload" \
      --write-out "\n%{http_code}\n" \
      --output "$temp_file" \
      2>&1)

    # Print the verbose output for debugging
    echo "DEBUG: Curl verbose output:"
    echo "$http_code" | head -n -2  # Print everything except last line (status code)

    # Get the actual status code (last line)
    status_code=$(echo "$http_code" | tail -n1)

    echo "DEBUG: HTTP Status code: $status_code"

    # Check response
    if [ -s "$temp_file" ]; then
        echo "DEBUG: Response body:"
        cat "$temp_file" | jq . 2>/dev/null || cat "$temp_file"
    fi

    # Check if successful
    if [ "$status_code" -ge 200 ] && [ "$status_code" -lt 300 ]; then
        echo "SUCCESS: HTTP $status_code"
        rm -f "$temp_file"
        return 0
    else
        echo "ERROR: HTTP $status_code"
        rm -f "$temp_file"
        return 1
    fi

}

check_and_index () {
    # Check if indexd record exist.
    # If record it exists, check for correct url, correct url if incorrect.
    # If record doesn't exist then create a new indexd record with aws url and gdc api

    # Assign Values
    did=$1

    # Make the request and capture response with status code
    response=$(curl --request GET \
        --max-time 10 \
        --retry 5 \
        --retry-delay 10 \
        --retry-max-time 40 \
        --silent \
        --write-out "\n%{http_code}" \
        --url "$HOSTNAME/index/$did")

    # Extract status code (last line) and body (everything except last line)
    status_code=$(echo "$response" | tail -n 1)
    body=$(echo "$response" | head -n 1)

    echo "Status code: $status_code"
    if [ "$status_code" = "200" ]; then
        did=$(echo "$body" | jq -r '.did')
        urls=$(echo "$body" | jq -r '.urls')
        array_length=$(echo "$urls" | jq '. | length')
        echo "Array has $array_length items"

        s3_exists=false

        # Iterate by index
        for ((i=0; i<array_length; i++)); do
            url=$(echo "$urls" | jq -r ".[$i]")
            echo "URL $((i+1)): $url"
            if [ "$url" = "$S3_OBJ" ]; then
                echo "AWS url matches correct url"
                s3_exists=true
                break
            fi
        done

        if ! $s3_exists ; then
            echo "IndexD record exists but no correct AWS url"
            putRecord "$body" "$S3_OBJ"
        else
            echo "IndexD record exists and AWS URL already correct, no update required..."
        fi

    else
        echo "IndexD record not found. Creating a new record"
        postRecord $did $FILE_NAME $SIZE $MD5SUM $S3_OBJ $AUTHZ $ACL "https://api.gdc.cancer.gov/data/$did"
        # Add your POST logic here when ready
    fi
}

# --------------------------------------- End of Helper Functions --------------------------------------- #


while [ "$attempt" -le "$MAX_RETRIES" ]; do

    MD5_FILE="$(mktemp /tmp/md5.XXXXXX)"
    SIZE_FILE="$(mktemp /tmp/size.XXXXXX)"

    aws_cp_cmd=(aws s3 cp - "$S3_OBJ")
    if [ -n "${SIZE:-}" ]; then
        aws_cp_cmd+=(--expected-size "$SIZE")
    fi

    if [ -n "${PROFILE_NAME:-}" ]; then
        aws_cp_cmd+=(--profile "$PROFILE_NAME")
    fi

    if curl --fail --location "https://api.gdc.cancer.gov/data/$ID" \
             --header "X-Auth-Token: $GDC_TOKEN" \
        | tee >(md5sum | awk '{print $1}' > "$MD5_FILE") \
        | tee >(wc -c    | awk '{print $1}' > "$SIZE_FILE") \
        | "${aws_cp_cmd[@]}"; then


        downloaded_size="$(cat "$SIZE_FILE")"
        downloaded_md5="$(cat "$MD5_FILE")"

        rm -f "$MD5_FILE" "$SIZE_FILE"

        size_ok=true
        md5_ok=true

        if [ -n "${SIZE:-}" ]; then
            if [ "$downloaded_size" -ne "$SIZE" ]; then
                echo "Size mismatch: Expected $SIZE bytes, got $downloaded_size bytes"
                size_ok=false
            else
                echo "Size validation passed: $downloaded_size bytes"
            fi
        else
            echo "SIZE not set, skipping size validation (observed: $downloaded_size bytes)"
        fi

        if [ -n "${MD5SUM:-}" ]; then
            if [ "$downloaded_md5" != "$MD5SUM" ]; then
                echo "md5sum mismatch: Expected $MD5SUM, got $downloaded_md5"
                md5_ok=false
            else
                echo "MD5 validation passed: $downloaded_md5"
            fi
        else
            echo "MD5SUM not set, skipping MD5 validation (observed: $downloaded_md5)"
        fi

        if [ "$size_ok" = true ] && [ "$md5_ok" = true ]; then
            echo "Download validation passed, upload complete."
            success=true
            break
        else
            echo "Validation failed, removing possibly corrupt S3 object: $S3_OBJ"
            aws s3 rm "$S3_OBJ" || true
        fi

    else
        echo "curl/pipe/aws s3 cp pipeline failed"
        rm -f "$MD5_FILE" "$SIZE_FILE" || true
    fi

    echo "Attempt $attempt failed, sleeping $RETRY_DELAY seconds then retrying..."
    sleep "$RETRY_DELAY"
    attempt=$((attempt + 1))
done

if [ "$success" = false ]; then
    echo "ERROR: File transfer failed after $MAX_RETRIES attempts"
    exit 1
fi

echo "Checking indexd for record"
check_and_index $ID

echo "SUCCESS: File verified and transferred"
exit 0
