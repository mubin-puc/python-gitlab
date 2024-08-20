import requests
import json
import boto3
import sys
import settings
import pandas as pd
import numpy as np
import time
from conf.logger_config import log_info, log_error, log_success,log_msg
from common_utils import upload_json_to_s3

if len(sys.argv) < 2:
    # print("Usage:  python script.py <input_value>")
    sys.exit(1)
timestamp_value = sys.argv[1]
num_of_days = settings.num_of_days

start_time = time.time()
headers = {
    "Authorization": f"Bearer {settings.conf_data['bearer_token']}"
    }
def get_launcher_data():
    base_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4/types/description/"
    endpoints = settings.launcher_details['modernized_launcher_list']
    launcher_ids = []
    launcher_descriptions = []

    for endpoint in endpoints:
        product_line = endpoint.split("- ")[1].rstrip("/")
        log_info(f"Productline Modernized: {product_line}", timestamp_value)
        url = base_endpoint + endpoint
        response = requests.get(url, headers=headers)
        launcher_data = json.loads(response.text)
        
        if "id" in launcher_data:
            launcher_id = launcher_data["id"]
            launcher_description = launcher_data["description"]
            launcher_ids.append(launcher_id)
            launcher_descriptions.append(launcher_description)
            log_info(f"Modernized Launcher Id: {launcher_id}", timestamp_value)
            log_info(f"Modernized Launcher Description: {launcher_description}", timestamp_value)
        else:
            # pass
            log_error(f"Modernized Launcher data not found for {product_line}.", timestamp_value)
        log_msg("---------------------------------")
    return launcher_ids, launcher_descriptions
launcher_ids, launcher_descriptions = get_launcher_data()


def get_assembly_details():
    endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Assemblies/userpackages"
    assemblies = settings.launcher_details['assembly_list']
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
            package_details.append({"Package ID": package_id, "Package Name": package_name, "Product Line": product_line})
    package_details.sort(key=lambda x: x["Product Line"])
    log_info(f"Modernized Assembly details are:", timestamp_value)
    for detail in package_details:
        log_msg(detail)
    return package_details
package_details = get_assembly_details()
print("----package details---", package_details)

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

                package_name = next((pkg['Package Name'] for pkg in package_details if pkg['Package ID'] == package_id), None)

                if package_name and package_name not in package_tag_ids:
                    package_tag_ids[package_name] = {'Package ID': package_id, 'Signal Tag IDs': [], 'Event Tag IDs': [], 'Signal Tag Names': {}, 'Signal Tag Aliases': {}}

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

assembly_ids_integrated = get_assembly_tree(package_details, launcher_ids)
result = get_tags_endpoints(assembly_ids_integrated, package_details)
for package_name, package_data in result.items():
    log_info("Modernized Package Details: ", timestamp_value)
    log_msg(f"Package Name: {package_name}")
    log_msg(f'Package ID: {package_data["Package ID"]}')
    log_msg(f"Signal Tag IDs: {package_data['Signal Tag IDs']}")
    log_msg(f"Event Tag IDs: {package_data['Event Tag IDs']}")
    log_msg(f"----------------------------------------------")

def get_signal_data(tag_ids):
    signal_data_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/data"
    signal_data = []

    # Call the get_tags_endpoints function to retrieve tag information
    package_tag_ids = get_tags_endpoints(assembly_ids_integrated, package_details)

    for i in range(0, len(tag_ids), 25):
        chunk = tag_ids[i:i+25]
        params = {
            "TagIds": ",".join(str(tag_id) for tag_id in chunk),
            "From": settings.conf_data['from_date'],
            "To": settings.conf_data['to_date'],
            "Timeout": 60
        }

        response = requests.get(signal_data_endpoint, params=params, headers=headers)
        if response.status_code == 200:
            chunk_data = response.json()

            for data in chunk_data:
                if data is not None and 'tagId' in data:
                    tag_id = data['tagId']
                    tag_name = None
                    tag_alias = None

                    # Retrieve the tag name and alias from package_tag_ids
                    for package_data in package_tag_ids.values():
                        if tag_id in package_data['Signal Tag IDs']:
                            tag_name = package_data['Signal Tag Names'].get(tag_id)
                            tag_alias = package_data['Signal Tag Aliases'].get(tag_id)
                            break

                    # Add the tag name and alias to the signal data
                    data['tagName'] = tag_name if tag_name is not None else "None"
                    data['tagAlias'] = tag_alias if tag_alias is not None else "None"

            signal_data.extend(chunk_data)
        else:
            log_error(f"Failed to retrieve Modernized signal data for chunk: {chunk}")
    return signal_data


def get_signal_data_24h(tag_ids):
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
        chunk = tag_ids[i:i+25]
        params = {
            "TagIds": ",".join(str(tag_id) for tag_id in chunk),
            "From": settings.conf_data['from_date'],
            "To": settings.conf_data['to_date'],
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
                merged_df[columns_to_merge]=None
            else:
                df_tagendpoint_subset = df_tagendpoint[columns_to_merge]
                merged_df = pd.merge(df_result, df_tagendpoint_subset, on='tagId', how='inner')
            merged_df.replace({np.nan: None}, inplace=True)
            result_list = merged_df.to_dict(orient='records')
            signal_data.extend(result_list)
        else:
            log_error(f"Failed to retrieve Legacy signal data for chunk: {chunk}", timestamp_value)
    return signal_data

def get_signal_data_long_date_test(tag_ids, assembly_ids_integrated):
    signal_data = []
    # Call the get_tags_endpoints function to retrieve tag information
    #package_tag_ids = get_tags_endpoints(assembly_ids_integrated, package_details)
    integrated_assembly_id_list = []
    for i in assembly_ids_integrated:
        integrated_assembly_id_list.append(i['Assembly ID (Integrated)'])
    tag_endpoint_response_list = []
    tags_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/tags/?DatasourceId={}"
    for i in integrated_assembly_id_list:
        tag_endpoint_url = tags_endpoint.format(i)
        response = requests.get(tag_endpoint_url,headers=headers)
        tag_endpoint_response_list.append(json.loads(response.text))
    # print("--tag_endpoint_response_list--")
    # print(tag_endpoint_response_list)

    failed_tag_obj = {"failed_tag_ids":[]}
    for tag_id in range(0, len(tag_ids)):
        # print(f'iteration {tag_id}')
        signal_data_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/tags/{}/data"
        signal_data_endpoint = signal_data_endpoint.format(tag_ids[tag_id])

        params = {
            "From": settings.conf_data['from_date'],
            "To": settings.conf_data['to_date'],
            "Timeout": 60,
            "filter": "ALL"
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
            failed_tag_obj["failed_tag_ids"].append(tag_ids[tag_id])
    if len(failed_tag_obj["failed_tag_ids"]) > 0:
        log_error(f"Failed to retrieve Legacy signal data for: {failed_tag_obj['failed_tag_ids']}")
    return signal_data
def get_event_data(package_details):
    events_endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Events"
    event_data = {}

    for package in package_details:
        package_id = package['Package ID']
        package_name = package['Package Name']
        event_data[package_name] = []

        # Fetch event data for the package ID
        events_url = f"{events_endpoint}?packageId={package_id}&from={settings.conf_data['timestamp_value']}&to={settings.conf_data['timestamp_value']}&tagIdentifierOption=TagName"
        response = requests.get(events_url, headers=headers)

        if response.status_code == 200:
            events = response.json()

            for event in events:
                event_data[package_name].append(event)

    return event_data

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
            agent_messages_url = f"{agent_messages_endpoint}?assemblyId={package_id}&from=2024-05-31T11:30:00&to=2024-06-01T11:30:00"
            response = requests.get(agent_messages_url,headers=headers)

            if response.status_code == 200:
                messages = response.json()

                for message in messages:
                    if message['agentId'] == agent_id:
                        filtered_message = message.copy()
                        agent_messages[product_line][package_name].append(filtered_message)

                        agent_messages[product_line][package_name].append(filtered_message)

    return agent_messages

def generate_output_data(package_details, agent_ids):
    package_tag_ids = get_tags_endpoints(assembly_ids_integrated, package_details)

    output_data = []

    for package in package_details:
        product_line = package['Product Line']
        package_name = package['Package Name']
        package_id = package['Package ID']
        tag_ids = package_tag_ids[package_name]['Signal Tag IDs']
        event_data = get_event_data([package])
        agent_messages = get_agent_messages([package], agent_ids)

        package_data = {
            'Product Line': product_line,
            'Package Name': package_name,
            'Package ID': package_id,
            'Signal Data': get_signal_data_24h(tag_ids) if num_of_days<1 else get_signal_data_long_date_test(tag_ids,assembly_ids_integrated),
            'Event Data': event_data.get(package_name, []),
            'Agent Messages': agent_messages.get(product_line, {}).get(package_name, {})
        }

        output_data.append(package_data)

    return output_data

# Define the agent IDs for each product line
agent_ids = {
    "AGT": 43302,
    "MGT": 43304,
    "IST": 43303,
    "STC": 43307,
    "RCE": 43305,
    "SGT": 43306
}

try:
    output_data = generate_output_data(package_details, agent_ids)
    log_success(f'Signal Data generated for Modernized DMA', timestamp_value)
    log_success(f'Event Data generated for Modernized DMA', timestamp_value)
    log_success(f'Agent Messages Data generated for Modernized DMA', timestamp_value)
    #For local testing uncomment this and comment upload_json_to_s3 function
    with open('input_json/test_DMA_Modernized.json', 'w') as file:
        json.dump(output_data, file, indent=4)
    s3_bucket_name = 'sdo-dma-dev-fra-se-data-copy'
    s3 = boto3.client('s3')
    # upload_json_to_s3(s3_bucket_name=s3_bucket_name, output_data=output_data, filename="DMA_Modernized.json", timestamp=timestamp_value)

except Exception as e:
    log_error(e, timestamp_value)

end_time = time.time()
execution_time = end_time - start_time
log_info(f"Execution time Modernized:  {execution_time} \"seconds\"", timestamp_value)