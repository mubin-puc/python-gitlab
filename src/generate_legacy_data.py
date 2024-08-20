import requests
import json
import boto3
from datetime import datetime
import settings
import pandas as pd
import numpy as np
import time
from conf.logger_config import log_info, log_error, log_msg, log_success
from delete_task import DeleteData

num_of_days = settings.num_of_days

start_time = time.time()

# sta-rms test is using UTC for creation time utc_start_time is used to filter AM messages on creation time.
utc_start_time = datetime.utcnow()

headers = {
    "Authorization": f"Bearer {settings.conf_data['bearer_token']}"
}
timestamp_value = settings.conf_data['timestamp_value']


class LegacyLauncherDetails:
    def __init__(self):
        self.launcher_list_legacy = settings.launcher_details['legacy_launcher_list']
        self.assemblies_legacy = settings.launcher_details['assembly_list']
    def get_legacy_launcher_data(self):
        base_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4/types/description/"
        endpoints = self.launcher_list_legacy
        launcher_ids = []
        launcher_descriptions = []

        for endpoint in endpoints:
            product_line = endpoint.replace("DMA Launcher - ", "").replace(" Agent Launcher", "").rstrip("/")
            log_info(f"Productline Legacy: {product_line}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
            url = base_endpoint + endpoint
            response = requests.get(url, headers=headers)
            launcher_data = json.loads(response.text)

            if "id" in launcher_data:
                launcher_id = launcher_data["id"]
                launcher_description = launcher_data["description"]
                launcher_ids.append(launcher_id)
                launcher_descriptions.append(launcher_description)

                log_info(f"Legacy Launcher Id:, {launcher_id}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                log_info(f"Legacy Launcher Description: {launcher_description}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')

            else:
                pass
                log_error(f"Launcher data not found for {product_line}.",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')

            print("---------------------------------")
        return launcher_ids, launcher_descriptions

    def get_assembly_details(self):
        endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Assemblies/userpackages"
        assemblies = self.assemblies_legacy
        response = requests.get(endpoint, headers=headers)
        json_data = json.loads(response.text)
        package_details = []

        for assembly in json_data:
            if assembly["packageName"] in assemblies:
                package_id = assembly["packageId"]
                package_name = assembly["packageName"]
                product_line = assembly.get("productLine")
                if product_line is None:
                    product_line = "None"
                package_details.append(
                    {"Package ID": package_id, "Package Name": package_name, "Product Line": product_line})
        package_details.sort(key=lambda x: x["Product Line"])
        log_info(f"Legacy Assembly details are:",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        for detail in package_details:
            log_msg(detail)
            return package_details


    @staticmethod
    def get_assembly_tree(package_details, launcher_ids):
        tree_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Assemblies/{}/tree"
        assembly_ids_integrated = []

        for package in package_details:
            package_id = package["Package ID"]
            package_name = package["Package Name"]
            package_tree_endpoint = tree_endpoint.format(package_id)
            response = requests.get(package_tree_endpoint, headers=headers)
            tree_data = json.loads(response.text)

            def traverse_tree(node, package_id):
                if "assemblyTypeId" in node and node["assemblyTypeId"] in launcher_ids:
                    result = {
                        'Package Name': package_name,
                        'Package ID': package_id,
                        'Assembly Name': node.get('assemblyName'),
                        'Assembly ID (Integrated)': node.get('assemblyId')
                    }
                    assembly_ids_integrated.append(result)

                if "children" in node:
                    for child in node["children"]:
                        traverse_tree(child, package_id)

            traverse_tree(tree_data[0], package_id)

        # print("-------------------------------------------------------")
        # print("Modernized Assembly IDs (Integrated with Launcher Type):")
        sorted_assemblies = sorted(assembly_ids_integrated, key=lambda x: x['Assembly Name'])
        # for detail in sorted_assemblies:
        #     print(detail)
        # print("-------------------------------------------------------")
        return sorted_assemblies

    @staticmethod
    def get_tags_endpoints(assembly_ids_integrated, package_details):
        tags_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/?DatasourceId={}"
        tag_types_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/tagtypes"
        package_tag_ids = {}

        # Fetch tag types
        tag_types_response = requests.get(tag_types_endpoint, headers=headers)
        if tag_types_response.status_code == 200:
            tag_types_data = tag_types_response.json()

            for assembly_id in assembly_ids_integrated:
                package_id = assembly_id['Package ID']
                assembly_id_integrated = assembly_id['Assembly ID (Integrated)']
                tags_endpoint_url = tags_endpoint.format(assembly_id_integrated)
                response = requests.get(tags_endpoint_url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    package_name = next(
                        (pkg['Package Name'] for pkg in package_details if pkg['Package ID'] == package_id),
                        None)
                    if package_name and package_name not in package_tag_ids:
                        package_tag_ids[package_name] = {'Package ID': package_id, 'Signal Tag IDs': [],
                                                         'Event Tag IDs': [], 'Signal Tag Names': {},
                                                         'Signal Tag Aliases': {}}

                    for tag in data:
                        tag_id = tag['tagId']
                        tag_type_id = tag.get('tagTypeId')
                        matching_tag_type = next((t for t in tag_types_data if t['id'] == tag_type_id), None)
                        if matching_tag_type:
                            if matching_tag_type.get('signalType') == 'Signal':
                                package_tag_ids[package_name]['Signal Tag IDs'].append(tag_id)
                                package_tag_ids[package_name]['Signal Tag Names'][tag_id] = tag.get('tagName')
                                package_tag_ids[package_name]['Signal Tag Aliases'][tag_id] = tag.get('tagAlias')
                            else:
                                package_tag_ids[package_name]['Event Tag IDs'].append(tag_id)

        return package_tag_ids

class UpdateLegacyTaskDetails:
    def __init__(self, launcher_type):
        self.launcher_type = launcher_type

    def get_legacy_all_open_tasks_list(self):
        list_all_tasks_endpoint = 'https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/StartDate'
        params = {
            "from": "2023-08-01T00:00:00",
            "to": datetime.utcnow(),
            "timeout": 60
        }
        legacy_all_open_tasks_list_df = pd.DataFrame()
        response = requests.get(list_all_tasks_endpoint, params=params, headers=headers)
        if response.status_code == 200:
            df_tasks_list = pd.DataFrame(response.json())
            legacy_launcher_name = f'DMA Launcher -'
            filtered_open_tasks_list_df = df_tasks_list[(df_tasks_list.statusId == 265) &
                                                        (df_tasks_list['description'].str.contains(
                                                            legacy_launcher_name)) & (~df_tasks_list['description'].str.contains("DMA Launcher - Test"))]
            filtered_unique_open_tasks_list_df = filtered_open_tasks_list_df.drop_duplicates(subset='startDate',
                                                                                             keep='first')
            if filtered_unique_open_tasks_list_df.empty:
                log_info(
                    f"No open tasks found in DMA Legacy Launcher to close..",
                    datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                filtered_closed_task_list_df = df_tasks_list[(df_tasks_list.statusId == 266) &
                                                             (df_tasks_list['description'].str.contains(
                                                                 legacy_launcher_name))]
                filtered_unique_closed_tasks_list_df = filtered_closed_task_list_df.drop_duplicates(subset='startDate',
                                                                                                    keep='first')
                legacy_all_open_tasks_list_df = pd.concat([legacy_all_open_tasks_list_df, filtered_unique_closed_tasks_list_df],
                                                 ignore_index=True)
            else:
                legacy_all_open_tasks_list_df = pd.concat([legacy_all_open_tasks_list_df, filtered_unique_open_tasks_list_df],
                                                 ignore_index=True)
        else:
            log_error(f"Error occurred in fetching task_list : {response.status_code}",
                      datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')

        return legacy_all_open_tasks_list_df

    def get_legacy_open_tasks_list_enabled_assemblies(self, package_details):
        list_tasks_endpoint = 'https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/assembly/{}'
        params = {
            "from": settings.conf_data['from_date'],
            "to": settings.conf_data['to_date'],
        }
        legacy_tasks_list_df = pd.DataFrame()
        for packageId in package_details:
            endpoint_url = list_tasks_endpoint.format(packageId['Package ID'])
            response = requests.get(endpoint_url,params=params ,headers=headers)
            if response.status_code == 200:
                df_tasks_list = pd.DataFrame(response.json())
                legacy_launcher_name = f'DMA Launcher - {packageId["Product Line"]}'
                filtered_open_tasks_list_df = df_tasks_list[(df_tasks_list.statusId == 265) &
                                                    (df_tasks_list['description'].str.contains(legacy_launcher_name))]
                filtered_unique_open_tasks_list_df = filtered_open_tasks_list_df.drop_duplicates(subset='startDate', keep='first')
                if filtered_unique_open_tasks_list_df.empty:
                    log_info(f"No open tasks found in DMA Legacy Launcher for {packageId['Product Line']} - {packageId['Package Name']}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                    filtered_closed_task_list_df = df_tasks_list[(df_tasks_list.statusId == 266) &
                                  (df_tasks_list['description'].str.contains(legacy_launcher_name))]
                    filtered_unique_closed_tasks_list_df = filtered_closed_task_list_df.drop_duplicates(subset='startDate',
                                                                                                     keep='first')
                    legacy_tasks_list_df = pd.concat([legacy_tasks_list_df, filtered_unique_closed_tasks_list_df],
                                                     ignore_index=True)
                else:
                    legacy_tasks_list_df=pd.concat([legacy_tasks_list_df, filtered_unique_open_tasks_list_df],ignore_index=True)
            else:
                log_error(f"Error occurred in fetching task_list : {response.status_code}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        return legacy_tasks_list_df

    def get_task_status_legacy(self, specific_tasks_list_legacy):
        endpoint_url = 'https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/{}'
        task_status = []
        for each_task_id in specific_tasks_list_legacy:
            endpoint = endpoint_url.format(each_task_id)
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                obj = {'task_id':'','task_status':''}
                status = response.json()['statusId']
                if status==266:
                    obj['task_status']='closed'
                    obj['task_id']=each_task_id
                    task_status.append(obj)
                elif status==265:
                    obj['task_status']='open'
                    obj['task_id'] = each_task_id
                    task_status.append(obj)
            else:
                log_error(f"Error fetching modernized task status {response.status_code}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        return task_status

    def update_legacy_task_status(self, statusId, current_status_name, all_legacy_tasks_list_df=None,
                                  user_specific_tasks=None):
        if all_legacy_tasks_list_df is not None and user_specific_tasks is None:
            tasks_count = 0 if all_legacy_tasks_list_df is None else len(all_legacy_tasks_list_df['taskId'])
            if tasks_count != 0:
                log_info(f"{tasks_count} {current_status_name} tasks found for {self.launcher_type} launcher.",
                         datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),'DMA-Legacy',)
                for task_id in all_legacy_tasks_list_df['taskId']:
                    endpoint = f'https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/{task_id}/status'
                    params = {
                        "statusId": statusId
                    }
                    try:
                        if statusId == 265:
                            requests.put(endpoint, params=params, headers=headers)
                            log_info(f"Opened task for {task_id}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                        elif statusId == 266:
                            requests.put(endpoint, params=params, headers=headers)
                            log_info(f"Closed task for {task_id}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                    except Exception as e:
                       log_error(e,datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
            else:
                return
        elif all_legacy_tasks_list_df is None and user_specific_tasks is not None:
            tasks_count = len(user_specific_tasks)
            log_info(f"{tasks_count} {current_status_name} tasks found for DMA {self.launcher_type} launcher.",
                     datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),'DMA-Legacy')
            for task_id in user_specific_tasks:
                endpoint = f'https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/{task_id}/status'
                params = {
                    "statusId": statusId
                }
                try:
                    if statusId == 265:
                        requests.put(endpoint, params=params, headers=headers)
                        log_info(f"Opened task for {task_id}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                    elif statusId == 266:
                        requests.put(endpoint, params=params, headers=headers)
                        log_info(f"Closed task for {task_id}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                except Exception as e:
                    log_error(e,datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')

obj1 = UpdateLegacyTaskDetails('DMA-Legacy')
obj2 = LegacyLauncherDetails()
launcher_ids, launcher_descriptions = obj2.get_legacy_launcher_data()
package_details = obj2.get_assembly_details()
assembly_ids_integrated = obj2.get_assembly_tree(package_details, launcher_ids)
result = obj2.get_tags_endpoints(assembly_ids_integrated, package_details)
legacy_signal_tag_ids = []
legacy_event_tag_ids = []

for package_name, package_data in result.items():
    legacy_signal_tag_id_obj = {"package_name":package_name, "signal_tag_ids": []}
    legacy_event_tag_id_obj = {"package_name":package_name, "event_tag_ids": []}
    log_info(f"Legacy Package Details: ",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
    log_msg(f"Package Name: {package_name}")
    log_msg(f"Package ID: {package_data['Package ID']}")
    log_msg(f"Signal Tag IDs: {package_data['Signal Tag IDs']}")
    legacy_signal_tag_id_obj["signal_tag_ids"]=package_data.get('Signal Tag IDs')
    legacy_signal_tag_ids.append(legacy_signal_tag_id_obj)

    log_msg(f"Event Tag IDs: {package_data['Event Tag IDs']}")
    legacy_event_tag_id_obj["event_tag_ids"] = package_data.get('Event Tag IDs')
    legacy_event_tag_ids.append(legacy_event_tag_id_obj)
    log_msg(f"----------------------------------------------")


# Delete Signal and Event Data
delete_object = DeleteData(launcher_key='DMA-Legacy')
delete_object.delete_signal_data(legacy_signal_tag_ids)
delete_object.delete_event_data(legacy_event_tag_ids)

log_info(f"Fetching ALL open tasks for ALL assemblies for DMA legacy launcher", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')

# First get all the legacy task id list that are open for date range from 01Aug2023 to utc.now()
legacy_all_open_tasks_df = obj1.get_legacy_all_open_tasks_list()

# Close all open tasks for all assemblies
if not legacy_all_open_tasks_df.empty:
    if any(legacy_all_open_tasks_df['statusId'] == 265):
        # if true it enters here and since there are few open tasks, first close all the open tasks

        legacy_open_tasks_only_df = legacy_all_open_tasks_df[(legacy_all_open_tasks_df.statusId == 265)]
        log_info(f"{legacy_open_tasks_only_df.shape[0]} tasks are open for all the assemblies for DMA Legacy launcher", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        log_info(f"Closing ALL open tasks for ALL the assemblies for DMA Legacy launcher", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                 'DMA-Legacy')
        log_msg("----------------------------------------------")
        obj1.update_legacy_task_status(statusId=266, current_status_name="Open",
                                       all_legacy_tasks_list_df=legacy_open_tasks_only_df, user_specific_tasks=None)


legacy_tasks_list_enabled_assemblies_df = obj1.get_legacy_open_tasks_list_enabled_assemblies(package_details=package_details)
user_specific_tasks_legacy = settings.legacy_task_details
tasks_id_list = legacy_tasks_list_enabled_assemblies_df['taskId'] if settings.user_specified_task_id == 'FALSE' \
                else user_specific_tasks_legacy


if not legacy_tasks_list_enabled_assemblies_df.empty:
    if any(legacy_tasks_list_enabled_assemblies_df['statusId'] == 265):
        # if true it enters here and since there are few open tasks, first close all the open tasks
        obj1.update_legacy_task_status(statusId=266, current_status_name="Open",
                                       all_legacy_tasks_list_df=legacy_tasks_list_enabled_assemblies_df, user_specific_tasks=None)

# Next check if user_specified_task_id=TRUE/FALSE
if settings.user_specified_task_id=='TRUE':
    # user_specified_task is TRUE
    # check if user has actually specified any task_ids
    if settings.legacy_task_details == []:
        #even though user_specified_task_id is TRUE user has not passed any task_ids in csv hence reopen all tasks for enabled assemblies.
        log_info(f"Reopening tasks for enabled assemblies for DMA legacy launcher", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                 'DMA-Legacy')
        log_msg("----------------------------------------------")
        obj1.update_legacy_task_status(statusId=265, current_status_name="Closed",
                                       all_legacy_tasks_list_df=legacy_tasks_list_enabled_assemblies_df, user_specific_tasks=None)
    else:
        # user has passed specific task ids as inputs hence only re-open those specific tasks
        obj1.update_legacy_task_status(statusId=265, current_status_name="Closed",
                                       all_legacy_tasks_list_df=None, user_specific_tasks=tasks_id_list)
else:
    # user_specified_task is FALSE
    # Reopen all tasks which are closed in given from and to_date range in appconf.json under general_config.
    log_info(f"Reopening DMA Legacy tasks for enabled assemblies for date range {settings.conf_data['from_date']} to "
             f"{settings.conf_data['from_date']}", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
             'DMA-Legacy')
    log_msg("----------------------------------------------")
    obj1.update_legacy_task_status(statusId=265, current_status_name="Closed",
                                   all_legacy_tasks_list_df=legacy_tasks_list_enabled_assemblies_df, user_specific_tasks=None)

#Wait for Legacy Tasks to get closed
class LegacyTaskStatusMonitor():
    def __init__(self):
        self.task_status_legacy= UpdateLegacyTaskDetails('DMA-Legacy')

    def monitor_status(self):
        log_info("Waiting for all DMA Legacy tasks to close...", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                 'DMA-Legacy')
        #print("Waiting for all DMA legacy tasks to close...")
        while True:
            legacy_task_statuses = self.task_status_legacy.get_task_status_legacy(tasks_id_list)
            if all(task['task_status'] =='closed' for task in legacy_task_statuses):
                break
            else:
                log_info("Checking latest task status for legacy DMA...",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
                time.sleep(5)
        log_info(f'All DMA legacy tasks are closed {legacy_task_statuses}',datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        log_info(f'Waiting for {settings.conf_data["wait_time_seconds"]} seconds after closing the Legacy tasks..',
                 datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),'DMA-Legacy')
        time.sleep(int(settings.conf_data['wait_time_seconds']))
        return None

obj3 = LegacyTaskStatusMonitor()
obj3.monitor_status()


class GenerateLegacyData:

    def __init__(self):
        self.get_tags_endpoints = LegacyLauncherDetails.get_tags_endpoints(assembly_ids_integrated, package_details)
        self.legacy_agent_ids = settings.legacy_agent_ids
    @staticmethod
    def get_signal_data_24h(tag_ids, task_id_start_date, task_id_end_date):
        signal_data_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/data"
        signal_data = []
        # Call the get_tags_endpoints function to retrieve tag information
        # package_tag_ids = get_tags_endpoints(assembly_ids_integrated, package_details)
        integrated_assembly_id_list = []
        for i in assembly_ids_integrated:
            integrated_assembly_id_list.append(i['Assembly ID (Integrated)'])
        tag_endpoint_response_list = []
        tags_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/?DatasourceId={}"
        for i in integrated_assembly_id_list:
            tag_endpoint_url = tags_endpoint.format(i)
            response = requests.get(tag_endpoint_url, headers=headers)
            tag_endpoint_response_list.append(json.loads(response.text))
        for i in range(0, len(tag_ids), 25):
            chunk = tag_ids[i:i + 25]
            params = {
                "TagIds": ",".join(str(tag_id) for tag_id in chunk),
                "From": task_id_start_date,
                "To": task_id_end_date,
                "Timeout": 60
            }
            response = requests.get(signal_data_endpoint, params=params, headers=headers)
            if response.status_code == 200:
                result = response.json()
                result = map(lambda x: {} if x is None else x, result)
                df_result = pd.DataFrame(result)
                df_tagendpoint = pd.DataFrame(tag_endpoint_response_list[0])
                columns_to_merge = ['tagId', 'tagName', 'tagAlias']
                if df_tagendpoint.empty:
                    merged_df = df_result
                    merged_df[columns_to_merge] = None
                else:
                    df_tagendpoint_subset = df_tagendpoint[columns_to_merge]
                    merged_df = pd.merge(df_result, df_tagendpoint_subset, on='tagId', how='inner')
                merged_df.replace({np.nan: None}, inplace=True)
                result_list = merged_df.to_dict(orient='records')
                signal_data.extend(result_list)
            else:
                log_error(f"Failed to retrieve Legacy signal data for chunk: {chunk} {response.status_code}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
        return signal_data

    # @staticmethod
    # def get_signal_data_long_date_test(tag_ids, assembly_ids_integrated):
    #     signal_data = []
    #     # Call the get_tags_endpoints function to retrieve tag information
    #     # package_tag_ids = get_tags_endpoints(assembly_ids_integrated, package_details)
    #     integrated_assembly_id_list = []
    #     for i in assembly_ids_integrated:
    #         integrated_assembly_id_list.append(i['Assembly ID (Integrated)'])
    #     tag_endpoint_response_list = []
    #     tags_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/?DatasourceId={}"
    #     for i in integrated_assembly_id_list:
    #         tag_endpoint_url = tags_endpoint.format(i)
    #         response = requests.get(tag_endpoint_url, headers=headers)
    #         tag_endpoint_response_list.append(json.loads(response.text))
    #     # print("--tag_endpoint_response_list--")
    #     # print(tag_endpoint_response_list)
    #
    #     failed_tag_obj = {"failed_tag_ids": []}
    #     for tag_id in range(0, len(tag_ids)):
    #         # print(f'iteration {tag_id}')
    #         signal_data_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/tags/{}/data"
    #         signal_data_endpoint = signal_data_endpoint.format(tag_ids[tag_id])
    #
    #         params = {
    #             "From": settings.conf_data['from_date'],
    #             "To": settings.conf_data['to_date'],
    #             "Timeout": 60,
    #             "filter": "ALL"
    #         }
    #         response = requests.get(signal_data_endpoint, params=params, headers=headers)
    #         if response.status_code == 200:
    #             result = response.json()
    #             result = map(lambda x: {} if x is None else x, result)
    #             df_result = pd.DataFrame(result)
    #             df_tagendpoint = pd.DataFrame(tag_endpoint_response_list[0])
    #             columns_to_merge = ['tagId', 'tagName', 'tagAlias']
    #             if df_tagendpoint.empty:
    #                 merged_df = df_result
    #                 merged_df[columns_to_merge] = None
    #             else:
    #                 df_tagendpoint_subset = df_tagendpoint[columns_to_merge]
    #                 merged_df = pd.merge(df_result, df_tagendpoint_subset, on='tagId', how='inner')
    #             merged_df.replace({np.nan: None}, inplace=True)
    #             result_list = merged_df.to_dict(orient='records')
    #             signal_data.extend(result_list)
    #         else:
    #             failed_tag_obj["failed_tag_ids"].append(tag_ids[tag_id])
    #     if len(failed_tag_obj["failed_tag_ids"]) > 0:
    #         log_error(f"Failed to retrieve Legacy signal data for: {failed_tag_obj['failed_tag_ids']}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
    #     return signal_data

    @staticmethod
    def get_event_data(package_details,task_id_start_date, task_id_end_date):
        events_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Events"
        event_data = {}

        for package in package_details:
            package_id = package['Package ID']
            package_name = package['Package Name']
            event_data[package_name] = []

            # Fetch event data for the package ID
            events_url = f"{events_endpoint}?packageId={package_id}&from={task_id_start_date}&to={task_id_end_date}&tagIdentifierOption=TagName"
            response = requests.get(events_url, headers=headers)

            if response.status_code == 200:
                events = response.json()
                for event in events:
                    event_data[package_name].append(event)
            else:
                log_error(f"Error fetching event data {response.status_code}",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),"DMA-Legacy")
        return event_data

    @staticmethod
    def get_agent_messages(package_details, agent_ids):
        agent_messages_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/agentmessages/daterange"
        agent_messages = {}

        for package in package_details:
            package_id = package['Package ID']
            package_name = package['Package Name']
            product_line = package['Product Line']
            agent_id = agent_ids.get(product_line)

            if agent_id is not None:
                agent_messages[product_line] = agent_messages.get(product_line, {})
                agent_messages[product_line][package_name] = []

                # Fetch agent messages for the package ID
                to_date = datetime.strptime(timestamp_value,"%Y-%m-%d_%H-%M-%S")
                from_date = utc_start_time
                agent_messages_url = f"{agent_messages_endpoint}?&assemblyId={package_id}&from={from_date}&to={to_date}&SearchUsingCreationTime=true"
                response = requests.get(agent_messages_url, headers=headers)

                if response.status_code == 200:
                    messages = response.json()

                    for message in messages:
                        if message['agentId'] == agent_id:
                            filtered_message = message.copy()
                            agent_messages[product_line][package_name].append(filtered_message)

        return agent_messages

    def generate_output_data(self, package_details):
        package_tag_ids = self.get_tags_endpoints
        output_data = []
        for package in package_details:
            log_msg(message=f'Generating Signal, Event and Agent Message Data for Legacy DMA for {package}')
            product_line = package['Product Line']
            package_name = package['Package Name']
            package_id = package['Package ID']
            tag_ids = package_tag_ids[package_name]['Signal Tag IDs']
            task_id_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4/tasks/{}"
            signal_data = []
            event_data = []
            for task_id in tasks_id_list:
                endpoint = task_id_endpoint.format(task_id)
                response_tasks = requests.get(endpoint, headers=headers)
                result = response_tasks.json()
                task_id_start_date = datetime.fromisoformat(result['startDate']).strftime("%Y-%m-%dT%H:%M:%S")
                task_id_end_date = datetime.fromisoformat(result['endDate']).strftime("%Y-%m-%dT%H:%M:%S")
                signal_data_result  = GenerateLegacyData.get_signal_data_24h(tag_ids,task_id_start_date,
                                                                             task_id_end_date)
                signal_data.extend(signal_data_result)
                event_data_result = GenerateLegacyData.get_event_data(package_details,task_id_start_date,
                                                                      task_id_end_date)
                event_data.extend(event_data_result.get(package_name, []))

            # event_data = GenerateLegacyData.get_event_data([package])
            agent_messages = GenerateLegacyData.get_agent_messages([package], self.legacy_agent_ids)

            package_data = {
                'Product Line': product_line,
                'Package Name': package_name,
                'Package ID': package_id,
                'Signal Data': signal_data,
                'Event Data': event_data,
                'Agent Messages': agent_messages.get(product_line, {}).get(package_name, {})
            }
            output_data.append(package_data)
        return output_data


obj4 = GenerateLegacyData()


try:
    output_data = obj4.generate_output_data(package_details)

    log_success(f'Signal Data generated for Legacy DMA',datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
    log_success(f'Event Data generated for Legacy DMA',datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
    log_success(f'Agent Messages Data generated for Legacy DMA',datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
    # For local testing uncomment this and comment upload_json_to_s3 function
    with open('input_json/test_DMA_Legacy.json', 'w') as file:
        json.dump(output_data, file, indent=4)

    s3_bucket_name = 'sdo-dma-dev-fra-se-data-copy'
    s3 = boto3.client('s3')
    # upload_json_to_s3(s3_bucket_name=s3_bucket_name, output_data=output_data, filename="DMA_Legacy.json",
    #                   timestamp=timestamp_value)
except Exception as e:
    log_error(e,datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
#
end_time = time.time()
execution_time = end_time - start_time
#
log_info(f"Execution time Legacy:  {execution_time} \"seconds\"",datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), 'DMA-Legacy')
