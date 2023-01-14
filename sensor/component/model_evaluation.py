from sensor.entity import config_entity,artifact_entity
from sensor.predictor import ModelResolver
from sensor.exception import SensorException
import os,sys
from sensor.utils import load_object
from sklearn.metrics import f1_score
import pandas as pd
import sys,os
from sensor.config import TARGET_COLUMN
from sensor.logger import logging
class ModelEvaluation:

    def __init__(self,model_eval_config:config_entity.ModelEvaluationConfig,
        data_ingestion_artifact:artifact_entity.DataIngestionArtifact,
        data_transform_artifact:artifact_entity.DataTransformationArtifact,
        model_trainer_artifact:artifact_entity.ModelTrainerArtifact):
        try:
            logging.info(f"{'>>'*20} Model Evaluation {'<<'*20}")
            self.model_resolver = ModelResolver()
            self.model_eval_config = model_eval_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_transform_artifact = data_transform_artifact
            self.model_trainer_artifact = model_trainer_artifact
        except Exception as e:
            raise SensorException(e,sys)
    
    def initate_model_evalution(self):
        try:
            logging.info("initate_model_evalution started")
            latest_path_dir = self.model_resolver.get_latest_dir_path()
            if latest_path_dir is None:
                model_eval_artifact = artifact_entity.ModelEvaluationArtifact(is_model_accepted=True, improved_accuracy=None)
                logging.info(f"Model evaluation artifact: {model_eval_artifact}")
                return model_eval_artifact

            #finding location of transformer, modeel, target encoder
            logging.info("finding location of transformer, modeel, target encoder")
            transformer_path=self.model_resolver.get_latest_transformer_path()
            model_path=self.model_resolver.get_latest_model_path()
            target_encoder_path=self.model_resolver.get_latest_target_encoder_path()

            #previous trained objects
            logging.info("previous trained objects")
            transformer=load_object(file_path=transformer_path)
            model = load_object(file_path=model_path)
            target_encoder = load_object(file_path=target_encoder_path)

            #currently trained objects
            logging.info("currently trained objects")
            current_transformer = load_object(file_path=self.data_transform_artifact.transform_object_path)
            current_model = load_object(file_path=self.model_trainer_artifact.model_path)
            current_target_encoder = load_object(file_path=self.data_transform_artifact.target_encoder_path)

            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            target_df = test_df[TARGET_COLUMN]
            y_true = target_encoder.transform(target_df)

            #accuracy using previously trained model
            logging.info("accuracy using previously trained model")
            input_feature_name = list(transformer.feature_names_in_)
            input_arr = transformer.transform(test_df[input_feature_name])
            y_pred = model.predict(input_arr)
            print(f"prediction using previous model {target_encoder.inverse_transform(y_pred[:5])}")
            previous_model_score = f1_score(y_true,y_pred)

            #accuracy using current trained model
            logging.info("accuracy using current trained model")
            input_feature_name = list(current_transformer.feature_names_in_)
            input_arr = current_transformer.transform(test_df[input_feature_name])
            y_pred = model.predict(input_arr)
            print(f"prediction using previous model {current_target_encoder.inverse_transform(y_pred[:5])}")
            y_true = current_target_encoder.transform(target_df)

            current_model_score = f1_score(y_true,y_pred)

            if(current_model_score<previous_model_score):
                logging.info("Current trained model is better than previous model")
                raise Exception("Current trained model is better than previous model")
            model_evaluation_artifact = artifact_entity.ModelEvaluationArtifact(is_model_accepted=True, improved_accuracy=(current_model_score-previous_model_score))

            logging.info(f"model_evaluation_artifact {model_evaluation_artifact}")
            return model_evaluation_artifact
        except Exception as e:
            raise SensorException(e, sys)