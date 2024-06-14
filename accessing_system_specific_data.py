import requests
import warnings
import logging
import json
import os

import pandas as pd

from os.path import abspath
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from specklepy.api.client import SpeckleClient
from specklepy.api.credentials import get_account_from_token
from speckle_automate import (
    AutomationContext
)


class AccessSystemSpecificData:
    
    def __init__(self, stream_url, stream_id, server, token) -> None:
        self.stream_url = stream_url
        self.stream_id = stream_id
        self.server = server
        self.token = token

    def read_query(self, query_file):
        current_dir = Path(query_file).parent.absolute()
        query_path = abspath(current_dir.as_posix() + '/graphql/' + query_file)
        return open(query_path, 'r').read()
    
    def get_graphql_query_response(self, query, variables=None, query_name=None):
        response = requests.post(self.server, json={
            "query": query,
            "operationName": query_name,
            "variables": variables or {}
        }, headers={
            "Authorization": f"Bearer {self.token}",
            "content-type": "application/json",
            "accept": "application/json"
        })

        return response
    
    def get_graphql_query_response_as_json(self, response):
        json_response = response.json()
        if "errors" in json_response:
            print(f"GraphQL response returned errors: {json_response['errors']}")
        if "data" not in json_response:
            if "errors" in json_response:
                print(f"GraphQL response returned errors: {json_response['errors']}")
            raise Exception("GraphQL response has no data!")
        return response.json()["data"]
    
    def get_query_response(self, query_file, variables=None, query_name= None):
        stream_query = self.read_query(query_file)
        response = self.get_graphql_query_response(stream_query, variables, query_name)
        json_response = self.get_graphql_query_response_as_json(response)

        return json_response
    
    def get_list_of_commit_object_ids(self, stream_response): 
        commit_object_ids = []
        stream_response_data = stream_response['project']['versions']['items']
        for element in stream_response_data:
            commit_object_ids.append(element['referencedObject'])

        return commit_object_ids
    

    def get_commit_data_dictionary(self, commit_response):
        commit_data = commit_response['stream']['object']['children']['objects']
        return commit_data
    
    def extract_id_type(self, row):
        object_id_value = row['data']['id']
        speckle_type_value = row['data']['speckle_type']
        return object_id_value, speckle_type_value
    
    def create_speckle_data_dataframe(self, commit_data_dictionary, commit_object_ids):
        data_list = []

        for dictionary in commit_data_dictionary:
            data = {"speckle_data" : {"Model URL": self.stream_url, "Version Object ID": commit_object_ids[0], "data": dictionary}}
            data_list.append(data)

        data_df = pd.DataFrame(data_list)
        data_df = data_df.speckle_data.apply(pd.Series)

        data_df['Object ID'], data_df['speckle_type'] = zip(*data_df['data'].apply(self.extract_id_type))

        data_df = data_df[['Model URL', 'Version Object ID', 'Object ID', 'speckle_type', 'data']]

        return data_df
    
    def groupby_system_classification(self, df):
        systems_data = {}

        for _, row in df.iterrows():
            stream_url = row['Model URL']
            commit_object_id = row['Version Object ID']
            object_id = row['Object ID']
            speckle_type = row['speckle_type']
            data = row['data']['data'] 

            if 'parameters' in data.keys():
                parameters = data['parameters']
                classification_desc = None
                other_params = {
                    'Model URL': stream_url,
                    'Version Object ID': commit_object_id,
                    'Object ID': object_id,
                    'speckle_type': speckle_type
                }

                for param_name, param_info in parameters.items():
                    if isinstance(param_info, dict):
                        if param_info['name'] == 'Classification.Uniclass.Ss.Description':
                            classification_desc = param_info['value']
                        else:
                            param_label = param_info['name']
                            if param_info['units'] != None:
                                param_label += f" ({param_info['units']})"
                            other_params[param_label] = param_info['value']

                if classification_desc:
                    if classification_desc not in systems_data:
                        systems_data[classification_desc] = []

                    other_params['Classification.Uniclass.Ss.Description'] = classification_desc
                    systems_data[classification_desc].append(other_params)

        # Convert the grouped data dictionary to separate DataFrames
        systems_dfs = {}
        for classification_desc, desc_data in systems_data.items():
            systems_dfs[classification_desc] = pd.DataFrame(desc_data)

        return systems_dfs
    
    def truncate_sheet_name(self, sheet_name):
        return sheet_name[:31]

    def export_to_excel(self, dataframes_dict, excel_filename):
        with pd.ExcelWriter(excel_filename, engine='xlsxwriter') as writer:
            for sheet_name, df in dataframes_dict.items():
                truncated_sheet_name = self.truncate_sheet_name(sheet_name)
                df.to_excel(writer, sheet_name=truncated_sheet_name, index=False)

    def process_speckle_data(self):
        stream_query_variables = {"streamId": f"{self.stream_id}"}
        stream_json_response = self.get_query_response(query_file='GetStreamQuery.graphql', variables=stream_query_variables, query_name='Stream')
        commit_object_ids = self.get_list_of_commit_object_ids(stream_json_response)

        commit_query_variables = {"streamId": f"{self.stream_id}",
                            "objectId": commit_object_ids[0]}
        commit_response_json = self.get_query_response(query_file='GetCommitQuery.graphql', variables=commit_query_variables, query_name='Commit')
        commit_data_dictionary = self.get_commit_data_dictionary(commit_response_json)
        
        data_df = self.create_speckle_data_dataframe(commit_data_dictionary=commit_data_dictionary, commit_object_ids=commit_object_ids)

        systems_df = self.groupby_system_classification(data_df)

        self.export_to_excel(dataframes_dict=systems_df, excel_filename='Systems_data.xlsx')
