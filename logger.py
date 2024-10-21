import logging
import json
from datetime import datetime
import random
from pytz import UTC
import os

# Create a custom logging handler
class JsonFileHandler(logging.FileHandler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            with open(self.baseFilename, 'a') as f:
                f.write(log_entry + '\n')
        except Exception as e:
            print(f"Error writing log entry: {e}")

# Set up the logger
logger = logging.getLogger('jsonLogger')
logger.setLevel(logging.DEBUG)

# Change the log file path to a directory with write permissions
log_file_path = os.getenv('LOG_FILE_PATH', 'C:\\Users\\MMadan\\OneDrive - Rockwell Automation, Inc\\Desktop\\Project1\\json_logs.log')
json_handler = JsonFileHandler(log_file_path)
json_handler.setLevel(logging.DEBUG)

# Create a custom formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'Username': getattr(record, 'username', 'unknown'),
            'Log_id': getattr(record, 'log_id', random.randint(1000, 9999)),
            'Timestamp': datetime.now(UTC).isoformat(),
            'Values': getattr(record, 'values', {})
        }
        return json.dumps(log_record)

json_handler.setFormatter(JsonFormatter())
logger.addHandler(json_handler)
