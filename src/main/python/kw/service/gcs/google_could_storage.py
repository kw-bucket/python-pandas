import os

from google.cloud import storage

class GoogleCloudStorage:
  def __init__(self) -> None:
    self.client = storage.Client()
    self.bucket = self.client.bucket(os.getenv('bucketName'))

  def upload(self, src: str) -> None:
    blob = self.bucket.blob(src)
    blob.upload_from_filename(src)

    print(' Uploaded! {0}'.format(src))
