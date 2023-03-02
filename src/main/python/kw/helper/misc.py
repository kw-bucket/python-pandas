import json
import os

def extract_schema_columns(schema_file_path: str) -> list:
    with open(schema_file_path) as json_file:
      schema = json.load(json_file)
    json_file.close

    return list(map(lambda obj: obj['name'], schema))

def create_dir(path: str):
  if not os.path.exists(path):
    os.makedirs(path)
