import json

from datetime import datetime
from bson import ObjectId

class MongoJSONEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, ObjectId):
      return str(obj)
    if isinstance(obj, datetime):
      return obj.astimezone().strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
    return json.JSONEncoder.default(self, obj)
