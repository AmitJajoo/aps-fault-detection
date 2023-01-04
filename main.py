import pymongo
from sensor.utils import get_collection_name
from sensor.exception import SensorException
import os,sys
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.component.data_ingestion import DataIngestion
from sensor.component.data_validation import DataValidation
from sensor.component.data_tranformation import DataTransformation
from sensor.component.model_trainer import ModelTrainer

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
          data_validation_artifact = data_validation.initiate_data_column()

          data_transformation_config = config_entity.DataTransformationConfig(training_config_pipeline=training_pipeline_config)
          data_transformation = DataTransformation(data_transformation_config=data_transformation_config, data_ingestion_artifact=data_ingestion_artifact)
          data_transformation_artifact = data_transformation.initiate_data_transformation()

          model_trainer_config = config_entity.ModelTrainerConfig(training_config_pipeline=training_pipeline_config)
          model_trainer = ModelTrainer(model_trainer_config=model_trainer_config, data_transform_artifact=data_transformation_artifact)
          model_trainer_artifact = model_trainer.initiate_model_transform()
          
     except Exception as e:
          raise SensorException(e, sys)
