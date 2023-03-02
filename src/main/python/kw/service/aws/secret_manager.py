import boto3
import base64
import os
import json

from botocore.exceptions import ClientError

class SecretManager:
  def __init__(self) -> None:
    self.secret_name = os.getenv('AWS_SECRET_ID')
    self.region_name = os.getenv('AWS_REGION')

    self.session = boto3.session.Session()
    self.client = self.session.client(service_name='secretsmanager', region_name=self.region_name)

    self.secret_dict = None

  def secrets(self) -> dict:
    self.secret_dict = self.__get_secret_dict() if self.secret_dict is None else self.secret_dict
    return self.secret_dict

  def __get_secret_dict(self) -> dict:
    try:
      response = self.client.get_secret_value(SecretId=self.secret_name)
    except ClientError as e:
      print('Get secret failure! {0}'.format(e.response['Error']['Code']))
      exit(0)
    else:
      if 'SecretString' in response:
        secret = response['SecretString']
      else:
        secret = base64.b64decode(response['SecretBinary'])

    return json.loads(secret)
