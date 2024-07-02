"""Run integration tests with a speckle server."""
import pytest
import os
from pydantic import SecretStr

from speckle_automate import (
    AutomationContext,
    AutomationRunData,
    AutomationStatus,
    run_function
)

# from main import FunctionInputs, automate_function
from main import automate_function

from speckle_automate.fixtures import *

from dotenv import load_dotenv

load_dotenv()

custom_cert_path = os.getenv("SPECKLE_SSL_CERT")
# print(custom_cert_path)
os.environ['CURL_CA_BUNDLE'] = custom_cert_path

# test_automation_run_data and test_automation_token are functions from speckle_automate.fixtures
def test_function_run(test_automation_run_data: AutomationRunData, test_automation_token: str):
    """Run an integration test for the automate function."""

    automation_context = AutomationContext.initialize(
        test_automation_run_data, test_automation_token
    )
    automate_sdk = run_function(
        automation_context,
        automate_function,
        # FunctionInputs(
        #     user_token=SecretStr(os.getenv("SPECKLE_TOKEN")),
        # ),
    )

    assert automate_sdk.run_status == AutomationStatus.SUCCEEDED

