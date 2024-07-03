"""This module contains the business logic of the function.

Use the automation_context module to wrap your function in an Autamate context helper
"""
import os

# from dotenv import load_dotenv
from pydantic import Field, SecretStr, DirectoryPath 
from speckle_automate import (
    AutomateBase,
    AutomationContext,
    execute_automate_function,
)
from accessing_system_specific_data import AccessSystemSpecificData
from SpecklePy_accessing_system_specific_data import AccessSystemSpecificDataSpecklePy
from specklepy.api.client import SpeckleClient
from specklepy.api.credentials import get_account_from_token

# from flatten import flatten_base

# load_dotenv()

class FunctionInputs(AutomateBase):
    """These are function author defined values.

    Automate will make sure to supply them matching the types specified here.
    Please use the pydantic model schema to define your inputs:
    https://docs.pydantic.dev/latest/usage/models/
    """

    folder_path: str = Field(
        title="Insert the path to the folder where you want to save the Excel output.",
        # description="Ensure the folder path is enclosed in '' or  ""."
                    )


def automate_function(
    automate_context: AutomationContext,
    function_inputs: FunctionInputs,
) -> None:
    """This is an example Speckle Automate function.

    Args:
        automate_context: A context helper object, that carries relevant information
            about the runtime context of this function.
            It gives access to the Speckle project data, that triggered this run.
            It also has conveniece methods attach result data to the Speckle model.
        function_inputs: An instance object matching the defined schema.
    """

    print("The script has started.")
    client = automate_context.speckle_client
    # TOKEN = client.account.token
    # # token = function_inputs.user_token
    # print(f"Token in main: {TOKEN}")
    # PROJECT_ID = automate_context.automation_run_data.project_id
    # print(f"Project ID in main: {PROJECT_ID}")
    # SERVER_URL = client.account.serverInfo.url
    # print(f"Server URL in main {SERVER_URL}")
    # STREAM_URL = f"{SERVER_URL}/projects/{PROJECT_ID}"
    # print(f"Stream URL in main: {STREAM_URL}")

    # certificate = 'cacert.crt'
    # speckle_graphql = 'https://latest.speckle.systems/graphql'
    # os.environ['CURL_CA_BUNDLE'] = certificate

    # client = SpeckleClient(host=speckle_graphql)
    # print(client)
    # account = get_account_from_token(TOKEN, speckle_graphql)
    # print(account)

    # try:
    #     client.authenticate_with_account(account)
    # except Exception as e:
    #     automate_context.mark_run_failed(f"""SpeckleWarning: Possibly invalid token - could not authenticate Speckle Client for server {speckle_graphql}. 
    #                                      Error: {e}""")

    # access_system_data = AccessSystemSpecificData(stream_url=STREAM_URL, stream_id=PROJECT_ID, server=speckle_graphql, token=TOKEN)
    # systems_df = access_system_data.process_speckle_data()
    # print(f"Systems dataframe: {systems_df}")
    # access_system_data.export_to_excel(dataframes_dict=systems_df, excel_filename='systems_data.xlsx')

    
    project_id = automate_context.automation_run_data.project_id
    server = client.account.serverInfo.url
    token = client.account.token
    model_url = f"{server}/projects/{project_id}"

    """Most likely don't need to add this to the published function.
    FOR TESTING PURPOSES ONLY."""
    certificate = 'cacert.crt'
    os.environ['CURL_CA_BUNDLE'] = certificate


    access_system_data = AccessSystemSpecificDataSpecklePy(model_url=model_url, project_id=project_id, server=server, token=token)
    # systems_df = access_system_data.process_speckle_data(folder_path=DirectoryPath(function_inputs.folder_path))
    systems_df = access_system_data.process_speckle_data()
    print(f"Systems_df: {systems_df}")
    access_system_data.export_to_excel(dataframes_dict=systems_df, excel_filename='Systems_data.xlsx')
    


 



    # # if the function generates file results, this is how it can be
    # # attached to the Speckle project / model
    # # automate_context.store_file_result("./report.pdf")

    automate_context.store_file_result("./systems_data.xlsx")
    automate_context.mark_run_success("Data successfully extracted into Uniclass Systems.")
    # automate_context.set_context_view()

# make sure to call the function with the executor
if __name__ == "__main__":
    # NOTE: always pass in the automate function by its reference, do not invoke it!

    # pass in the function reference with the inputs schema to the executor
    execute_automate_function(automate_function, FunctionInputs)
    
    # execute_automate_function(automate_function)