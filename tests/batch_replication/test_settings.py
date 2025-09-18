INDEXD = {
    "host": "https://test.dev.planx-pla.net/index/index",
    "version": "v0",
    "auth": {"username": "", "password": ""},
}
PROJECT_ACL = {
    "ALICE": {
        "aws_bucket_prefix": "test-gdc-xyz-phs000111",
        "gs_bucket_prefix": "test-gdc-xyz-phs000111",
    },
    "BOB": {
        "aws_bucket_prefix": "test-gdc-abc-phs000222",
        "gs_bucket_prefix": "test-gdc-abc-phs000222",
    },
    "CHARLIE": {
        "aws_bucket_prefix": "test-gdc-def-phs000333",
        "gs_bucket_prefix": "test-gdc-def-phs000333",
    },
}
IGNORED_FILES = "/dcf-dataservice/ignored_files_manifest.csv"
DATA_ENDPT = "https://api.gdc.cancer.gov/data/"

IGNORED_FILES = "/dcf-dataservice/ignored_files_manifest.csv"

# By default the postfix for open buckets are -2 and no numerical postfix for controlled
# list of buckets that have -open and -controlled postfix
POSTFIX_1_EXCEPTION = ["test-gdc-xyz-phs000111"]
# list of buckets that have both -2-open and -2-controlled postfix
POSTFIX_2_EXCEPTION = ["test-gdc-abc-phs000222"]
