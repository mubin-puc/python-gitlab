import pandas as pd

import settings
import requests
import json
import time
from conf.logger_config import log_info, log_error, log_msg, log_success
headers = {
    "Authorization": f"Bearer {settings.conf_data['bearer_token']}"
}
timestamp_value = settings.conf_data['timestamp_value']
class DeleteData:
    def __init__(self, launcher_key):
        self.from_date = settings.conf_data['from_date']
        self.to_date = settings.conf_data['to_date']
        self.launcher_key = launcher_key
        self.timestamp_value = settings.conf_data['timestamp_value']
        # if launcher_key == 'DMA-Legacy':
        #     self.launcher = settings.launcher_details['legacy_launcher_list']
        # elif launcher_key  == 'DMA-K8s':
        #     self.launcher = settings.launcher_details['modernized_launcher_list']
        # else:
        #     log_error("error: Invalid launcher key. please pass either 'legacy' or 'modernized'", self.timestamp_value)


    def delete_signal_data(self, signal_tag_ids):
        endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/signals/tags/{}"
        params = {
            "From": self.from_date,
            "To": self.to_date
        }
        for i in signal_tag_ids:
            for count, signal_tag_id in enumerate(i['signal_tag_ids'], start=1):
                endpoint_url = endpoint.format(signal_tag_id)
                response = requests.delete(endpoint_url, params=params, headers=headers)
                if response.status_code == 200:
                    result = {
                        "message": f"Deleted {count} out of {len(i['signal_tag_ids'])} signal tag_id {signal_tag_id} for "
                                   f"{i['package_name']}",
                        "status": response.text,
                        "status_code": response.status_code
                    }
                    log_success(result, self.timestamp_value,self.launcher_key)
                else:
                    log_error(response.status_code, self.timestamp_value,self.launcher_key)

    def delete_event_data(self, event_tag_ids):
        endpoint = "https://test.sta-rms.siemens-energy.cloud/api/v4.5/Events/tags/{}"
        params = {
            "From": self.from_date,
            "To": self.to_date
        }
        for i in event_tag_ids:
            for count, event_tag_id in enumerate(i["event_tag_ids"], start=1):
                endpoint_url = endpoint.format(event_tag_id)
                response = requests.delete(endpoint_url, params=params, headers=headers)
                if response.status_code == 200:
                    result = {
                        "message": f" Deleted {count} out of {len(i['event_tag_ids'])} event tag_id {event_tag_id} "
                                   f"successfully for {i['package_name']}.",
                        "status": response.text,
                        "status_code": response.status_code
                    }
                    log_success(result, self.timestamp_value,self.launcher_key)
                else:
                    log_error(response.status_code, self.timestamp_value,self.launcher_key)



