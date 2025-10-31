import pytest
import os
import csv
from unittest.mock import patch
from batch_jobs.dcf_replication.dcf_replication_job import (
    parse_manifest_file,
    map_project_to_bucket,
    convert_file_info_to_output_manifest,
)


# Import the test settings
import tests.batch_replication.test_settings as test_settings

# Get path to test_manifest
TEST_MANIFEST_PATH = os.path.join(
    os.path.dirname(__file__), "data/GDC_test_manifest.tsv"
)

TEST_MANIFEST_PATH_2 = os.path.join(
    os.path.dirname(__file__), "data/GDC_test_manifest_2.tsv"
)


def test_parse_manifest_file_success():
    """
    Test successful parsing of manifest file using test_settings.
    """
    with patch(
        "batch_jobs.dcf_replication.dcf_replication_job.PROJECT_ACL",
        test_settings.PROJECT_ACL,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_1_EXCEPTION",
        test_settings.POSTFIX_1_EXCEPTION,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_2_EXCEPTION",
        test_settings.POSTFIX_2_EXCEPTION,
    ):

        # Parse the existing manifest file
        result = parse_manifest_file(TEST_MANIFEST_PATH)

        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 15

        # Check that all required fields are present
        for item in result:
            assert "id" in item
            assert "file_name" in item
            assert "size" in item
            assert "acl" in item
            assert "md5" in item
            assert "baseid" in item
            assert "url" in item
            assert "project_id" in item
            assert "destination_bucket" in item

            # Verify project IDs match expected values from test_settings
            assert item["project_id"] in test_settings.PROJECT_ACL.keys()
            assert item["acl"] in ["['open']", "['phs000178']"]


def test_map_project_to_bucket():
    """
    Test mapping project id to the correct AWS bucket
    """

    with patch(
        "batch_jobs.dcf_replication.dcf_replication_job.PROJECT_ACL",
        test_settings.PROJECT_ACL,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_1_EXCEPTION",
        test_settings.POSTFIX_1_EXCEPTION,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_2_EXCEPTION",
        test_settings.POSTFIX_2_EXCEPTION,
    ):

        result = parse_manifest_file(TEST_MANIFEST_PATH)
        for item in result:
            if item["acl"] == "['open']":
                if item["project_id"] == "ALICE":
                    assert item["destination_bucket"] == "test-gdc-xyz-phs000111-open"
                if item["project_id"] == "BOB":
                    assert item["destination_bucket"] == "test-gdc-abc-phs000222-2-open"
                if item["project_id"] == "CHARLIE":
                    assert item["destination_bucket"] == "test-gdc-def-phs000333-2-open"
            else:
                if item["project_id"] == "ALICE":
                    assert (
                        item["destination_bucket"]
                        == "test-gdc-xyz-phs000111-controlled"
                    )
                if item["project_id"] == "BOB":
                    assert (
                        item["destination_bucket"]
                        == "test-gdc-abc-phs000222-2-controlled"
                    )
                if item["project_id"] == "CHARLIE":
                    assert (
                        item["destination_bucket"]
                        == "test-gdc-def-phs000333-controlled"
                    )


def test_map_project_to_bucket_invalid_project():
    """
    Test bucket mapping with invalid project ID.
    """

    test_fi = {"project_id": "INVALID_PROJECT"}

    with patch(
        "batch_jobs.dcf_replication.dcf_replication_job.PROJECT_ACL",
        test_settings.PROJECT_ACL,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_1_EXCEPTION",
        test_settings.POSTFIX_1_EXCEPTION,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_2_EXCEPTION",
        test_settings.POSTFIX_2_EXCEPTION,
    ):

        with pytest.raises(ValueError, match="Project ID INVALID_PROJECT not found"):
            map_project_to_bucket(test_fi)


def test_parse_manifest_file_fields():
    """
    Test that parsed data contains correct field values using test_settings.
    """
    with patch(
        "batch_jobs.dcf_replication.dcf_replication_job.PROJECT_ACL",
        test_settings.PROJECT_ACL,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_1_EXCEPTION",
        test_settings.POSTFIX_1_EXCEPTION,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_2_EXCEPTION",
        test_settings.POSTFIX_2_EXCEPTION,
    ):

        result = parse_manifest_file(TEST_MANIFEST_PATH)

        alice_files = [item for item in result if item["project_id"] == "ALICE"]
        bob_files = [item for item in result if item["project_id"] == "BOB"]
        charlie_files = [item for item in result if item["project_id"] == "CHARLIE"]

        assert len(alice_files) == 5
        assert len(bob_files) == 5
        assert len(charlie_files) == 5

        # Verify URLs contain the expected pattern
        for item in result:
            assert item["url"].startswith("s3://test-gdc-bucket/")
            assert item["file_name"] == item["id"]

            # Verify destination buckets match expected patterns from test_settings
            if item["project_id"] == "ALICE":
                assert "test-gdc-xyz-phs000111" in item["destination_bucket"]
            elif item["project_id"] == "BOB":
                assert "test-gdc-abc-phs000222" in item["destination_bucket"]
            elif item["project_id"] == "CHARLIE":
                assert "test-gdc-def-phs000333" in item["destination_bucket"]


def test_manifest_file_content():
    """Test that the manifest file contains expected content structure."""
    # Verify the file exists
    assert os.path.exists(TEST_MANIFEST_PATH)

    # Read and verify the file structure
    with open(TEST_MANIFEST_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames

        # Verify all expected columns are present
        expected_columns = [
            "id",
            "file_name",
            "md5",
            "size",
            "state",
            "project_id",
            "baseid",
            "version",
            "release",
            "acl",
            "type",
            "deletereason",
            "url",
        ]
        for col in expected_columns:
            assert col in fieldnames

        # Verify there are rows in the file
        rows = list(reader)
        assert len(rows) > 0

        # Verify sample data matches expected project IDs
        project_ids = set(row["project_id"] for row in rows)
        assert project_ids.issubset(set(test_settings.PROJECT_ACL.keys()))


def test_convert_file_info_to_output_manifest():
    """Test the converted file_info have expected data format and value."""
    # Verify the file exists
    assert os.path.exists(TEST_MANIFEST_PATH)

    with patch(
        "batch_jobs.dcf_replication.dcf_replication_job.PROJECT_ACL",
        test_settings.PROJECT_ACL,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_1_EXCEPTION",
        test_settings.POSTFIX_1_EXCEPTION,
    ), patch(
        "batch_jobs.dcf_replication.dcf_replication_job.POSTFIX_2_EXCEPTION",
        test_settings.POSTFIX_2_EXCEPTION,
    ):
        results = []
        file_infos = parse_manifest_file(TEST_MANIFEST_PATH_2)
        for fi in file_infos:
            result = convert_file_info_to_output_manifest(fi)
            results.append(result)
        # compare with actual result
        assert results[0] == {
            "guid": "07de33ac-7a49-4008-b035-707129c02a1d",
            "md5": "53ac6ffda159e554622b0653a456ff9f",
            "size": "131",
            "authz": ["/open"],
            "acl": ["*"],
            "file_name": "07de33ac-7a49-4008-b035-707129c02a1d",
            "urls": [
                "https://api.gdc.cancer.gov/data/07de33ac-7a49-4008-b035-707129c02a1d",
                "s3://test-gdc-xyz-phs000111-open/07de33ac-7a49-4008-b035-707129c02a1d/07de33ac-7a49-4008-b035-707129c02a1d",
            ],
        }
        assert results[1] == {
            "guid": "d85d67aa-7273-403f-be77-9ef8ae998e4a",
            "md5": "0b1c5ce51087de69d08be74839d3714f",
            "size": "29",
            "authz": ["/open"],
            "acl": ["*"],
            "file_name": "d85d67aa-7273-403f-be77-9ef8ae998e4a",
            "urls": [
                "https://api.gdc.cancer.gov/data/d85d67aa-7273-403f-be77-9ef8ae998e4a",
                "s3://test-gdc-xyz-phs000111-open/d85d67aa-7273-403f-be77-9ef8ae998e4a/d85d67aa-7273-403f-be77-9ef8ae998e4a",
            ],
        }
        assert results[2] == {
            "guid": "9c53ffd6-ec96-4fa0-9f81-1ed6fa376380",
            "md5": "51ac986913cfd957c60372362d8490c9",
            "size": "42",
            "authz": ["/programs/phs000178"],
            "acl": ["phs000178"],
            "file_name": "9c53ffd6-ec96-4fa0-9f81-1ed6fa376380",
            "urls": [
                "https://api.gdc.cancer.gov/data/9c53ffd6-ec96-4fa0-9f81-1ed6fa376380",
                "s3://test-gdc-xyz-phs000111-controlled/9c53ffd6-ec96-4fa0-9f81-1ed6fa376380/9c53ffd6-ec96-4fa0-9f81-1ed6fa376380",
            ],
        }
        assert results[3] == {
            "guid": "4cb739ba-edc9-47a3-a395-154d039d5545",
            "md5": "3828ec2ac83976ff8b8e412d80489e99",
            "size": "166",
            "authz": ["/programs/phs000178", "/programs/phs000218"],
            "acl": ["phs000178", "phs000218"],
            "file_name": "4cb739ba-edc9-47a3-a395-154d039d5545",
            "urls": [
                "https://api.gdc.cancer.gov/data/4cb739ba-edc9-47a3-a395-154d039d5545",
                "s3://test-gdc-abc-phs000222-2-controlled/4cb739ba-edc9-47a3-a395-154d039d5545/4cb739ba-edc9-47a3-a395-154d039d5545",
            ],
        }
