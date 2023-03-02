import os
import json
import pytz

from datetime import datetime
from pymongo import MongoClient
from kw.json.mongo_json_encoder import MongoJSONEncoder
from kw.service.aws.secret_manager import SecretManager

class Extractor:
  def __init__(self) -> None:
    self.secret_manager = SecretManager()

    db_uri = os.getenv('DB_URI')
    db_username = self.__get_secret(key='DB_USERNAME')
    db_password = self.__get_secret(key='DB_PASSWORD')
    db_client = MongoClient(host = db_uri, username = db_username, password = db_password)

    self.db_collection = db_client['kw']['decision']

  def extract_data_dicts(self, start_datetime, end_datetime):
    query_string = {
      '$and': [
        {'request_time': {'$gte': start_datetime.astimezone(pytz.utc)}},
        {'request_time': {'$lte': end_datetime.astimezone(pytz.utc)}}
      ]
    }

    data_cursor = self.db_collection.find(query_string)

    return json.loads(MongoJSONEncoder().encode(list(data_cursor)))

  def __get_secret(self, key: str):
    secret_indicator = '{aws_secret}'
    v = os.getenv(key)

    if secret_indicator in v:
       secrets = self.secret_manager.secrets()
       v = v.replace(secret_indicator, '')
       return secrets[v]
    else:
       return v
