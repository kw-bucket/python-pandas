import pandas as pd
import hashlib
import json

from datetime import datetime
from kw.helper.misc import create_dir
from kw.service.gcs.google_could_storage import GoogleCloudStorage

class Loader:
  def __init__(self, bucket: str, as_of_datetime: datetime) -> None:
    self.bucket = bucket
    self.date = as_of_datetime
    self.date_str = self.date.strftime('%Y%m%d')
    self.gcs = GoogleCloudStorage()

  def load(self, df: pd.DataFrame, schema: str, table_name: str) -> None:
    gcs_path = f'{self.bucket}/{table_name}/{self.date.year}'
    create_dir(gcs_path)

    gcs_csv_filename = f'{table_name}_{self.date_str}_1.csv'
    gcs_csv_file_path = f'{gcs_path}/{gcs_csv_filename}'

    gcs_schema_filename = f'{table_name}_{self.date_str}.schema'
    gcs_schema_file_path = f'{gcs_path}/{gcs_schema_filename}'

    gcs_hash_filename = f'{table_name}_{self.date_str}.sha256'
    gcs_hash_file_path = f'{gcs_path}/{gcs_hash_filename}'

    self.__copy_schema(src=schema, dst=gcs_schema_file_path)

    hash_records = []
    if not df.empty:
      df.to_csv(gcs_csv_file_path, index=False)
      hash_records.append(f'{self.__sha256_file(gcs_csv_file_path)} {gcs_csv_filename}')

      self.gcs.upload(src=gcs_csv_file_path)

    hash_records.append(f'{self.__sha256_file(gcs_schema_file_path)} {gcs_schema_filename}')
    hash_records.append(f'total_records {len(df.index)}')

    with open(gcs_hash_file_path, 'w') as hash_file:
      for record in hash_records:
        hash_file.write(f'{record}\n')
    hash_file.close

    self.gcs.upload(src=gcs_schema_file_path)
    self.gcs.upload(src=gcs_hash_file_path)

  def __copy_schema(self, src, dst) -> None:
    with open(dst, 'w') as dst_file:
      json_file = open(src)
      data = json.load(json_file)
      json.dump(data, dst_file)
    dst_file.close

  def __sha256_file(self, file_path: str) -> str:
    with open(file_path, 'rb') as file:
      content = file.read()
      hash_content = hashlib.sha256(content).hexdigest()
    file.close

    return hash_content
