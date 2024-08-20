"""
APP Level configurations are defined here.
"""
import os
import sys
import configparser
import requests
from datetime import datetime
import pandas as pd
import numbers
import json


class Settings:
    def __init__(self, app_conf_path, csv_file_path, configjson_path):
        self.conf_file_path = app_conf_path
        self.csv_file_path = csv_file_path
        self.configjson_path = configjson_path
        self.parser = configparser.ConfigParser()
        self.parser.read(app_conf_path)

    def load_csv(self):
        """Load data from CSV"""
        self.df = pd.read_csv(self.csv_file_path)

    def load_json(self):
        with open(self.configjson_path, 'r') as json_file:
            self.config = json.load(json_file)

    def update_conf_file(self):
        """Update configuration file with product, assemblies, From and To Date values"""
        filtered_df = self.df[self.df['enabled'].str.lower()=='yes']
        products = filtered_df['product_line'].unique().tolist()
        self.config['data']['product_lines']=products
        assemblies = filtered_df['assembly'].unique().tolist()
        self.config['data']['assemblies'] = assemblies
        legacy_task_ids = filtered_df['legacy_task_ids'].dropna().tolist()
        user_specified_task_id = self.config['general_config']['user_specified_task_id']
        if user_specified_task_id=='TRUE':
            if legacy_task_ids != []:
                if isinstance(legacy_task_ids[0], numbers.Number):
                    legacy_task_ids = filtered_df['legacy_task_ids'].dropna().astype(int).tolist()

                    # legacy_task_details = filtered_df.apply(lambda row: {
                    #     'taskId': int(row['legacy_task_ids']),
                    #     'start_date': row['start_date'],
                    #     'end_date': row['end_date']
                    # }, axis=1).dropna().tolist()
                    self.config['data']['legacy_task_details'] = legacy_task_ids
                elif isinstance(legacy_task_ids[0], str):
                    legacy_task_ids = filtered_df['legacy_task_ids'].dropna().tolist()
                    # legacy_task_details = filtered_df.apply(lambda row: {
                    #     'taskId': row['legacy_task_ids'],
                    #     'start_date': row['start_date'],
                    #     'end_date': row['end_date']
                    # }, axis=1).tolist()
                    self.config['data']['legacy_task_details'] = legacy_task_ids
        elif user_specified_task_id=='FALSE':
            self.config['data']['legacy_task_details'] = []
        else:
            self.config['general_config']['user_specified_task_id'] = 'FALSE'

        k8s_task_ids = filtered_df['k8s_task_ids'].dropna().tolist()
        if user_specified_task_id == "TRUE":
            if k8s_task_ids:
                if isinstance(k8s_task_ids[0], numbers.Number):
                    k8s_task_ids = filtered_df['k8s_task_ids'].dropna().astype(int).tolist()
                    # k8s_task_details = filtered_df.apply(lambda row: {
                    #     'taskId': int(row['k8s_task_ids']),
                    #     'start_date': row['start_date'],
                    #     'end_date': row['end_date']
                    # }, axis=1).dropna().tolist()
                    self.config['data']['k8s_task_details'] = k8s_task_ids
                elif isinstance(k8s_task_ids[0], str):
                    k8s_task_ids = filtered_df['k8s_task_ids'].dropna().tolist()
                    # k8s_task_details = filtered_df.apply(lambda row: {
                    #     'taskId': int(row['k8s_task_ids']),
                    #     'start_date': row['start_date'],
                    #     'end_date': row['end_date']
                    # }, axis=1).tolist()
                    self.config['data']['k8s_task_details'] = k8s_task_ids
        elif user_specified_task_id=='FALSE':
            self.config['data']['k8s_task_details']  = []
        else:
            self.config['general_config']['user_specified_task_id'] = 'FALSE'
        with open(self.configjson_path,'w') as config_file:
            json.dump(self.config,config_file, indent=4)

        config_file.close()

    def get_conf_data(self):
        bearer_token = self.config['general_config']['bearer_token']
        user_specified_task_id = self.config['general_config']['user_specified_task_id']
        timestamp_value = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        product_lines = self.config['data']['product_lines']
        assembly_list = self.config['data']['assemblies']
        legacy_task_details = self.config['data']['legacy_task_details']
        k8s_task_details = self.config['data']['k8s_task_details']
        legacy_agent_ids = self.config['data']['legacy_agent_ids']
        k8s_agent_ids = self.config['data']['k8s_agent_ids']
        from_date = self.config['general_config']['from_date']
        to_date =self.config['general_config']['to_date']
        wait_time_seconds = self.config['general_config']['wait_time_seconds']
        result = {
            "bearer_token":bearer_token,
            "timestamp_value":timestamp_value,
            "product_lines":product_lines,
            "assembly_list":assembly_list,
            "legacy_agent_ids":legacy_agent_ids,
            "k8s_agent_ids":k8s_agent_ids,
            "from_date":from_date,
            "to_date":to_date,
            "wait_time_seconds":wait_time_seconds,
            'legacy_task_details':legacy_task_details,
            'k8s_task_details':k8s_task_details,
            'user_specified_task_id':user_specified_task_id
        }
        return result
    @staticmethod
    def get_launcher_assembly_details(product_lines, assembly_list):
        legacy_launcher_name = "DMA Launcher - "
        k8s_launcher_name = "DMA Agent Launcher K8S - "
        legacy_launcher_list = []
        k8s_launcher_list = []
        for i in product_lines:
            legacy_launcher_list.append(legacy_launcher_name+i)
            k8s_launcher_list.append(k8s_launcher_name+i)

        launcher_details = {"legacy_launcher_list": legacy_launcher_list,
                      "k8s_launcher_list": k8s_launcher_list,
                      "assembly_list": assembly_list}
        return launcher_details

    @staticmethod
    def get_num_of_days(from_date, to_date):
        from_date = datetime.fromisoformat(from_date)
        to_date = datetime.fromisoformat(to_date)
        delta = (to_date - from_date)
        num_of_days = delta.days
        return num_of_days

    @staticmethod
    def get_legacy_agent_ids(product_lines, legacy_agent_ids):
        agent_ids_object = {}
        for i in product_lines:
            agent_ids_object[i]=legacy_agent_ids.get(i)
        return agent_ids_object

    @staticmethod
    def get_k8s_agent_ids(product_lines, k8s_agent_ids):
        agent_ids_object = {}
        for i in product_lines:
            agent_ids_object[i] = k8s_agent_ids.get(i)
        return agent_ids_object

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_CONF_PATH = os.path.join(ROOT_DIR, r'conf\app.conf')
CSV_FILE_PATH = os.path.join(ROOT_DIR,r'conf\product_assembly_conf.csv')
CONFIG_JSON_PATH = os.path.join(ROOT_DIR, r'conf\appconf.json')

obj = Settings(APP_CONF_PATH,CSV_FILE_PATH,CONFIG_JSON_PATH)
obj.load_csv()
obj.load_json()
obj.update_conf_file()
conf_data = obj.get_conf_data()
launcher_details = obj.get_launcher_assembly_details(conf_data['product_lines'], conf_data['assembly_list'])
num_of_days = obj.get_num_of_days(conf_data['from_date'], conf_data['to_date'])
legacy_agent_ids = obj.get_legacy_agent_ids(conf_data['product_lines'], conf_data['legacy_agent_ids'])
k8s_agent_ids = obj.get_k8s_agent_ids(conf_data['product_lines'], conf_data['k8s_agent_ids'])
wait_time_seconds = conf_data['wait_time_seconds']
user_specified_task_id = conf_data['user_specified_task_id']
legacy_task_details = conf_data['legacy_task_details']
k8s_task_details = conf_data['k8s_task_details']

