from sensor.entity import artifact_entity
from sensor.entity import config_entity
from sensor.exception import SensorException
from sensor.logger import logging
import sys,os
import pandas as pd 
from typing import Optional
from sensor import utils
import numpy as np
from xgboost import XGBClassifier
from sklearn.metrics import f1_score

class ModelTrainer:
    def __init__(self,model_trainer_config:config_entity.ModelTrainerConfig,
    data_transform_artifact:artifact_entity.DataTransformationArtifact):
        try:
            logging.info(f"{'>>'*20} Model Training {'<<'*20}")
            self.model_trainer_config = model_trainer_config
            self.data_transform_artifact = data_transform_artifact
        except Exception as e:
            raise SensorException(e, sys)

    def train_model(self,x,y):
        try:
            xgb = XGBClassifier()
            xgb.fit(x,y)
            return xgb
        except Exception as e:
            raise SensorException(e, sys)

    def initiate_model_transform(self)->artifact_entity.ModelTrainerArtifact:
        try:
            train_arr = utils.load_numpy_arr_data(self.data_transform_artifact.transform_train_path)
            test_arr = utils.load_numpy_arr_data(self.data_transform_artifact.transform_test_path)

            logging.info(f"Splitting input and target feature from both train and test arr.")
            x_train,y_train = train_arr[:,:-1],train_arr[:,-1]
            x_test,y_test = test_arr[:,:-1],test_arr[:,-1]

            logging.info(f"Train the model")
            model = self.train_model(x=x_train,y=y_train)

            yhat_train = model.predict(x_train)
            logging.info("calculating the f1_train_score")
            f1_train_score = f1_score(y_train,yhat_train)

            logging.info("calculating the f1_test_score")
            yhat_test = model.predict(x_test)
            f1_test_score = f1_score(y_test,yhat_test)

            logging.info(f"f1_train_score: {f1_train_score} | f1_test_score: {f1_test_score} ")
            #check for overfitting or underfitting or expected score
            logging.info("Checking the model is underfitting or not")
            if(f1_test_score<self.model_trainer_config.expected_score):
                raise Exception(f"Model is not good as it is not able to give expected score {self.model_trainer_config.expected_score} : model acc score is {f1_test_score}")
            
            diff = abs(f1_train_score-f1_test_score)
            logging.info("Checking the model is overfitting or not")
            if diff>self.model_trainer_config.overfiting_thresh:
                raise Exception(f"Train and test diff {diff} is more than overfitting threshold {self.model_trainer_config.overfiting_thresh}")
            
            #save model
            utils.save_object(file_path=self.model_trainer_config.model_path, obj=model)

            #prepare artifact
            model_trainer_artifact=artifact_entity.ModelTrainerArtifact(model_path=self.model_trainer_config.model_path, f1_train_score=f1_train_score, f1_test_score=f1_test_score)
            logging.info(f"model_trainer_artifact : {model_trainer_artifact}")
            return model_trainer_artifact


        except Exception as e:
            raise SensorException(e, sys)
