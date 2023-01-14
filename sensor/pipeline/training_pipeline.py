from sensor.utils import get_collection_name
from sensor.exception import SensorException
import os,sys
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.component.data_ingestion import DataIngestion
from sensor.component.data_validation import DataValidation
from sensor.component.data_tranformation import DataTransformation
from sensor.component.model_trainer import ModelTrainer
from sensor.component.model_evaluation import ModelEvaluation
from sensor.component.model_pusher import ModelPusher


def start_traininig_pipeline():
    try:
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

        model_eval_config = config_entity.ModelEvaluationConfig(training_config_pipeline=training_pipeline_config)
        model_eval = ModelEvaluation(model_eval_config=model_eval_config, data_ingestion_artifact=data_ingestion_artifact, data_transform_artifact=data_transformation_artifact, model_trainer_artifact=model_trainer_artifact)
        model_eval_artifact = model_eval.initate_model_evalution()

        model_pusher_config = config_entity.ModelPusherConfig(training_config_pipeline=training_pipeline_config)
        model_pusher = ModelPusher(model_pusher_config=model_pusher_config, model_trainer_artifact=model_trainer_artifact, data_transform_artifact=data_transformation_artifact)
        model_pusher_artifact = model_pusher.initiate_model_pusher()

    except Exception as e:
        raise SensorException(e,sys)