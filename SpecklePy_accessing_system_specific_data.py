import os
import json

import pandas as pd

from specklepy.api.client import SpeckleClient
from specklepy.api.credentials import get_account_from_token

from specklepy.api import operations
from specklepy.objects.base import Base
from specklepy.api.wrapper import StreamWrapper
from specklepy.transports.server import ServerTransport
from specklepy.serialization.base_object_serializer import BaseObjectSerializer

from speckle_automate import AutomationContext

from dotenv import load_dotenv
from typing import Any, Dict, List, Tuple



class AccessSystemSpecificDataSpecklePy:

    def __init__(self, model_url, project_id, server, token) -> None:
        self.model_url = model_url
        self.project_id = project_id
        self.server = server
        self.token = token


    def get_speckle_client(self) -> SpeckleClient:
        """
        Authenticates a Speckle client using a host and token.

        Args:
            host (str): The host address for the Speckle server.
            token (str): The personal access token for authentication.

        Returns:
            SpeckleClient: An authenticated Speckle client.
        """
        client = SpeckleClient(host=self.server)
        client.authenticate_with_token(self.token)
        return client
    

    def get_version_object_id(self, client: SpeckleClient) -> str:
        """
        Get the latest version id of the Speckle model on which the automation is being triggered.

        Args:
            client (SpeckleClient): _description_
            project_id (str): _description_

        Returns:
            str: The most recent commit id
        """
        commits = client.commit.list(stream_id=self.project_id)
        latest_commit = commits[0].referencedObject # latest_commit is the same as version_object_id
        
        return latest_commit
    

    def create_transport_and_serializer(self, client: SpeckleClient) -> Tuple[ServerTransport, BaseObjectSerializer]:
        """
        Create the transport and serializer needed to access the model data.

        Args:
            client (SpeckleClient): _description_
            project_id (str): _description_

        Returns:
            Tuple[ServerTransport, BaseObjectSerializer]: _description_
        """
        transport = ServerTransport(client=client, stream_id=self.project_id)
        serializer = BaseObjectSerializer()

        return transport, serializer
    

    def get_base_object(self, latest_commit, transport):
        received_base = operations.receive(obj_id=latest_commit, remote_transport=transport)

        return received_base
    

    def get_properties(self, element: Base) -> dict:
        properties = {}
        for key in element.get_dynamic_member_names():
            properties[key] = getattr(element, key)
        for key in element.get_member_names():
            properties[key] = getattr(element, key)
        return properties
    

    def get_list_of_object_ids(self, base_object) -> List:
        """
        Iterates through the data in the model to get a list of the object ids.

        Args:
            base_object (_type_): The Speckle Base object

        Returns:
            List: A list of all the object ids.
        """

        properties = self.get_properties(base_object)

        object_ids = set()
        object_speckle_types = set()
        keys_with_data = ['@Materials', '@Views', '@Project Information', '@Sheets', 'elements']

        for key in properties.keys():
            for key in keys_with_data:
                for element in properties[key]:
                    object_ids.add(element.id)
                    object_speckle_types.add(element.speckle_type)

        object_ids = list(object_ids)

        types_properties = self.get_properties(base_object['@Types'])

        types_with_data = []
        types_object_ids = set()

        for type in types_properties.keys():
            if type.startswith('@'):
                types_with_data.append(type)

        for key in types_properties.keys():
            if key in types_with_data:
                for element in types_properties[key]:
                    types_object_ids.add(element.id)

        types_object_ids = list(types_object_ids)


        all_object_ids = object_ids + types_object_ids

        return all_object_ids
    


    def create_obj_id_data_dictionary(self, object_ids: List, transport: ServerTransport, serializer: BaseObjectSerializer) -> Dict:
        """_summary_

        Args:
            object_ids (List): _description_
            serializer (BaseObjectSerializer): _description_

        Returns:
            Dict: _description_
        """
        id_data_dictionary = {}

        for id in object_ids:
            data = operations.receive(id, transport)
            json_data = serializer.write_json(data)
            # print(json)
            dictionary = json.loads(json_data[1])
            id_data_dictionary[id] = dictionary

        return id_data_dictionary
    

    def create_speckle_data_dataframe(self, id_data_dictionary, version_object_id):
        """_summary_

        Args:
            id_data_dictionary (_type_): _description_
            version_object_id (_type_): _description_
            stream_url (_type_): _description_

        Returns:
            _type_: _description_
        """
        data_list = []

        for key, value in id_data_dictionary.items():
            data = {"speckle_data" : {"Model URL": self.model_url, "Version Object ID": version_object_id, "Object ID": key, "data": value}}
            data_list.append(data)

        data_df = pd.DataFrame(data_list)
        data_df = data_df.speckle_data.apply(pd.Series)

        # data_df['Object ID'], data_df['speckle_type'] = zip(*data_df['data'].apply(self.extract_id_type))

        data_df = data_df[['Model URL', 'Version Object ID', 'Object ID', 'data']]

        return data_df
    

    def groupby_system_classification(self, df) -> Dict:
        """_summary_

        Args:
            df (_type_): _description_

        Returns:
            Dict: _description_
        """
        systems_data = {}

        for _, row in df.iterrows():
            stream_url = row['Model URL']
            commit_object_id = row['Version Object ID']
            object_id = row['Object ID']
            # speckle_type = row['speckle_type']
            data = row['data']

            if 'parameters' in data.keys():
                parameters = data['parameters']
                classification_desc = None
                other_params = {
                    'Model URL': stream_url,
                    'Version Object ID': commit_object_id,
                    'Object ID': object_id,
                    # 'speckle_type': speckle_type
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
        # # Ensure the folder exists
        # if not os.path.exists(folder_path):
        #     os.makedirs(folder_path)
        
        # # Combine the folder path and filename
        # full_path = os.path.join(folder_path, excel_filename)

        with pd.ExcelWriter(excel_filename, engine='xlsxwriter') as writer:
            for sheet_name, df in dataframes_dict.items():
                truncated_sheet_name = self.truncate_sheet_name(sheet_name)
                df.to_excel(writer, sheet_name=truncated_sheet_name, index=False)

    def export_to_excel_with_folder_path(self, dataframes_dict, excel_filename, folder_path):
        # Ensure the folder exists
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        # Combine the folder path and filename
        full_path = os.path.join(folder_path, excel_filename)

        with pd.ExcelWriter(full_path, engine='xlsxwriter') as writer:
            for sheet_name, df in dataframes_dict.items():
                truncated_sheet_name = self.truncate_sheet_name(sheet_name)
                df.to_excel(writer, sheet_name=truncated_sheet_name, index=False)

    
    def process_speckle_data(self, folder_path):
    # def process_speckle_data(self):
        client = self.get_speckle_client()
        version_object_id = self.get_version_object_id(client)
        transport, serializer = self.create_transport_and_serializer(client)

        base_object = self.get_base_object(version_object_id, transport) 
        
        object_ids = self.get_list_of_object_ids(base_object)

        data_dictionary = self.create_obj_id_data_dictionary(object_ids, transport, serializer)
        
        
        data_df = self.create_speckle_data_dataframe(id_data_dictionary=data_dictionary, version_object_id=version_object_id)

        systems_df = self.groupby_system_classification(data_df)

        self.export_to_excel_with_folder_path(dataframes_dict=systems_df, excel_filename='Systems_data.xlsx', folder_path=folder_path)

        return systems_df





    
