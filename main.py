import pymongo
from sensor.utils import get_collection_name
from sensor.exception import SensorException
import os,sys
from sensor.entity import config_entity
from sensor.component.data_ingestion import DataIngestion
from sensor.entity import artifact_entity
from sensor.component.data_validation import DataValidation
# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

if __name__ == "__main__":
     try:
          # get_collection_name(database_name='aps', collection_name="sensor")
          training_pipeline_config = config_entity.TrainingPipelineConfig()
          data_ingestion_config  = config_entity.DataIngestionConfig(training_pipeline_config=training_pipeline_config)
          print(data_ingestion_config.to_dict())

          data_ingestion = DataIngestion(data_ingestion_config)
          data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
          
          data_validation_config = config_entity.DataValidationConfig(training_config_pipeline=training_pipeline_config)
          data_validation = DataValidation(data_validation_config=data_validation_config, data_ingestion_artifact=data_ingestion_artifact)
          data_validation.initiate_data_column()
          
     except Exception as e:
          SensorException(e, sys)
