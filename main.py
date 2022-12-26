import pymongo
from sensor.utils import get_collection_name
from sensor.exception import SensorException
import os,sys
# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

if __name__ == "__main__":
     try:
          get_collection_name(database_name='aps', collection_name="sensor")
     except Exception as e:
          SensorException(e, sys)
