from sensor.entity import artifact_entity
from sensor.entity import config_entity
from sensor.exception import SensorException
from sensor.logger import logging
import sys,os
from scipy.stats import ks_2samp
import pandas as pd 
from typing import Optional
from sensor import utils
import numpy as np
from sensor.config import TARGET_COLUMN

class DataValidation:

    def __init__(self,data_validation_config:config_entity.DataValidationConfig,
                data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error = dict()
        except Exception as e:
            raise SensorException(e, sys)

    def drop_missing_values_column(self,df:pd.DataFrame,report_key_name:str)->Optional[pd.DataFrame]:
        """
        This fuction will drop column which contains missing value more than specified threshold
        df: Accept a pandas DataFrame
        threshold:Precentage criteria to drop to a column
        =========================================================================================
        returns pd.DataFrame if atleast a single column is available after missing columns drop else None
        """

        try:
            threshold = self.data_validation_config.missing_threshold
            null_report = df.isna().sum()/df.shape[0]
            logging.info(f"selecting column name which contains null above to {threshold}")
            drop_column_name = null_report[null_report>threshold].index
            logging.info(f"Column to be drop: {list(drop_column_name)}")
            self.validation_error[report_key_name] = list(drop_column_name)
            df.drop(list(drop_column_name),axis=1,inplace=True)

            if len(df.columns) == 0:
                return None
            return df
        except Exception as e:
            raise SensorException(e,sys)

    def is_required_column_exists(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str)->bool:
        """
            checking number of column are equal in base df and train_df , test_df
        """
        try:
            base_columns = base_df.columns
            current_columns = current_df.columns
            print(f"base_column len {len(base_columns)} | current_column len {len(current_columns)}")
            missing_columns = []
            for base_column in base_columns:
                if base_column not in current_columns:
                    missing_columns.append(base_column)

            if len(missing_columns)>0:
                self.validation_error[report_key_name] = missing_columns
                return False
            return True

        except Exception as e:
            raise SensorException(e, sys)

    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str):
        try:
            drift_report = dict()
            base_columns = base_df.columns
            current_columns = current_df.columns

            for base_column in base_columns:
                base_data,current_data = base_df[base_column], current_df[base_column]
                #NUll huposthesis is that column data drawn 
                same_distribution = ks_2samp(base_data,current_data)

                if(same_distribution.pvalue>0.05):
                    #We are accepting null hypothesis
                    drift_report[base_column] = {
                        "pvalue":float(same_distribution.pvalue),
                        "same_distributin":True
                    }
                else:
                    drift_report[base_column] = {
                        "pvalue":float(same_distribution.pvalue),
                        "same_distributin":False
                    }
            self.validation_error[report_key_name] = drift_report
        except Exception as e:
            raise SensorException(e, sys)
    
    def initiate_data_column(self)->artifact_entity.DataValidationArtifact:
        try:
            logging.info(f"Reading base dataframe")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            logging.info(f"Replace the na value with np.NAN in base_df")
            base_df.replace({"na":np.NAN},inplace=True)
            #base_df has na as null 
            logging.info(f"Dropping null values from base_df")
            base_df=self.drop_missing_values_column(df=base_df,report_key_name="missing_value_within_base_dataset")

            logging.info(f"Reading train dataframe")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            logging.info(f"Reading test dataframe")
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            logging.info(f"Dropping null values from train_df")
            train_df = self.drop_missing_values_column(df=train_df,report_key_name="missing_value_within_train_dataset")
            logging.info(f"Dropping null values from test_df")
            test_df = self.drop_missing_values_column(df=test_df,report_key_name="missing_value_within_test_dataset")

            exclude_column = [TARGET_COLUMN]
            base_df=utils.convert_column_float(df=base_df, exclude_column=exclude_column)
            train_df=utils.convert_column_float(df=train_df, exclude_column=exclude_column)
            test_df=utils.convert_column_float(df=test_df, exclude_column=exclude_column)

            logging.info(f"is all required columns present in train_df")
            train_df_column_status = self.is_required_column_exists(base_df=base_df, current_df=train_df,report_key_name="missing_column_within_train_dataset")
            logging.info(f"is all required columns present in test_df")
            test_df_column_status = self.is_required_column_exists(base_df=base_df, current_df=test_df,report_key_name="missing_value_within_test_dataset")

            if train_df_column_status:
                logging.info(f"As all column are avaiable in train_df hence detecting data drift")
                self.data_drift(base_df=base_df, current_df=train_df,report_key_name="data_drift_within_train_dataset")
            if test_df_column_status:
                logging.info(f"As all column are avaiable in test_df hence detecting data drift")
                self.data_drift(base_df=base_df, current_df=test_df,report_key_name="data_drift_within_test_dataset")

            #write the yaml file
            logging.info("writing the yaml file")
            utils.write_yaml_file(file_path=self.data_validation_config.report_file_path, data=self.validation_error)

            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path=self.data_validation_config.report_file_path)
            logging.info(f"Data validation artifact : {data_validation_artifact}")
            return data_validation_artifact


        except Exception as e:
            raise SensorException(e, sys)