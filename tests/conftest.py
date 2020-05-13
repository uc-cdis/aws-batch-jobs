# contents of our test file e.g. test_code.py
import pytest

# -*- coding: utf-8 -*-
import sys
import os
import batch_jobs.bucket_manifest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def mock_env_user(monkeypatch):
    monkeypatch.setenv("ACCESS_KEY_ID", "ACCESS_KEY_ID")


@pytest.fixture
def mock_env_missing(monkeypatch):
    monkeypatch.delenv("USER", raising=False)
