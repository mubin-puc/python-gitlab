import logging

log_filename = f'automation_script.log'
logging.basicConfig(filename=f'logs/{log_filename}', level=logging.INFO)

def log_info(message, timestamp,type=None):
    if type==None:
        log_message=f"[INFO] [{timestamp}] {message}"
    else:
        log_message = f"[{type} - INFO] [{timestamp}] {message}"

    logging.info(log_message)
    print(log_message)

def log_msg(message,type=None):
    if type==None:
        log_message=message
    else:
        log_message = f'{type} - {message}'
    logging.info(log_message)
    print(log_message)
def log_error(message, timestamp,type=None,):
    if type==None:
        log_message=f"[ERROR] [{timestamp}] {message}"
    else:
        log_message = f"[{type} - ERROR] [{timestamp}] {message}"
    logging.error(log_message)
    print(log_message)

def log_success(message, timestamp,type=None):
    if type==None:
        log_message=f"[SUCCESS] [{timestamp}] {message}"
    else:
        log_message = f"[{type} - SUCCESS] [{timestamp}] {message}"
    logging.info(log_message)
    print(log_message)

