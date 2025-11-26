import sys
import os
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

INDEXD = {
    "host": "",
    "version": "",
    "auth": {"username": "", "password": ""},
}

SLACK_URL = ""

GDC_TOKEN = ""

POSTFIX_1_EXCEPTION = []

POSTFIX_2_EXCEPTION = []

try:
    with open("/secrets/dcf_dataservice_credentials.json", "r") as f:
        data = json.loads(f.read())
        INDEXD = data.get("INDEXD", {})
        SLACK_URL = data.get("SLACK_URL", "")
        GDC_TOKEN = data.get("GDC_TOKEN", "")
        POSTFIX_1_EXCEPTION = data.get("POSTFIX_1_EXCEPTION", [])
        POSTFIX_2_EXCEPTION = data.get("POSTFIX_2_EXCEPTION", [])
except Exception as e:
    print("Can not read dcf_dataservice_credentials.json file. Detail {}".format(e))


PROJECT_ACL = {}
try:
    with open("/dcf_dataservice/GDC_project_map.json", "r") as f:
        PROJECT_ACL = json.loads(f.read())
except Exception as e:
    print("Can not read GDC_project_map.json file. Detail {}".format(e))

IGNORED_FILES = "/dcf_dataservice/ignored_files_manifest.csv"

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"
