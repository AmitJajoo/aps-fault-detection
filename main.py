import pymongo

from sensor.pipeline.training_pipeline import start_traininig_pipeline
from sensor.pipeline.batch_prediction import batch_prediction

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")
file_name = "/config/workspace/aps_failure_training_set1.csv"
if __name__ == "__main__":
     try:
          # get_collection_name(database_name='aps', collection_name="sensor")
          # start_traininig_pipeline()
          batch_prediction(input_file_path=file_name)
     except Exception as e:
          raise SensorException(e, sys)
