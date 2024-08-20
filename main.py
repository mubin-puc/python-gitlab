import threading
from src.output_comparison import compare_output
import settings
import os
import subprocess
from conf.logger_config import log_info, log_error, log_success, log_msg
import subprocess
import os
from datetime import datetime


s3_bucket_name = 'sdo-dma-dev-fra-se-data-copy'
s3_prefix = 'output_comparison/'
script_dir = os.path.dirname(__file__)
input_folder_path = os.path.join(script_dir, 'input_json')

def run_script(script_name):
    message =f'Running {script_name}'
    log_info(message, settings.conf_data['timestamp_value'])
    subprocess.run(["python", script_name])


generate_legacy_data_script_path = os.path.join(script_dir, 'src','generate_legacy_data.py')
generate_modernized_data_script_path = os.path.join(script_dir, 'src','generate_modernized_data.py')

legacy_script_thread = threading.Thread(target=run_script,args=(generate_legacy_data_script_path,))
modernized_script_thread = threading.Thread(target=run_script, args=(generate_modernized_data_script_path,))

legacy_script_thread.start()
modernized_script_thread.start()

legacy_script_thread.join()
modernized_script_thread.join()

log_success(f"Both Legacy and Modernized Data json generation script has been executed.",
           datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))

#Below snippet is used for Comparing output_files that are generated in s3
log_success(f"Starting output comparison", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
compare_output(script_dir, s3_bucket_name, s3_prefix, input_folder_path, settings.conf_data['timestamp_value'])
