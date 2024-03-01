"""Tests for fileops-ValidationProcess lambda"""

from ..app import lambda_handler
from ..file_validations import get_file_encodings
from .utils import import_file as f

import json
from datetime import datetime
from pathlib import Path

ROOT_FOLDER = Path.cwd().parents[3]

event_str = f.import_file(f'{ROOT_FOLDER}/events/', 'fileopsValidationProcess.json')
event = json.loads(event_str)

try:
    result = lambda_handler(event, None)
    print(result)
    # folder_prefix = datetime.now().strftime("s3://filecentral-dev/%Y%m%d/sales-%Y%m%d.csv")
    # print(folder_prefix)
except Exception as e:
    print(f"Error: {e.with_traceback()}")
