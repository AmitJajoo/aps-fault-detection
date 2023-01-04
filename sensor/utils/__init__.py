import pandas as pd
from sensor.config import mongo_client
import os,sys
from sensor.exception import SensorException
from sensor.logger import logging
import yaml
import dill
import numpy as np 

def get_collection_name(database_name:str,collection_name:str)->pd.DataFrame:
    """
      Description:  this funcation return a collection as dataframe
      Params:  database_name: database_name
        collection_name: collection_name
        ==================================================
        return Pandas DataFrame of a collection

    """
    try:
        logging.info(f'Reading data from database {database_name} and collection {collection_name}')
        df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f'Found Columns {df.columns}')
        if "_id" in df.columns:
            logging.info(f"Dropping column: _id")
            df = df.drop("_id",axis=1)
        return df
    except Exception as e:
        raise SensorException(e, sys)

def write_yaml_file(file_path,data:dict):
    try:
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir,exist_ok=True)
        with open(file_path,'w') as file_writer:
            yaml.dump(data,file_writer)
    except Exception as e:
        raise SensorException(e, sys)

def convert_column_float(df:pd.DataFrame,exclude_column:list)->pd.DataFrame:
    try:
        for column in df.columns:
            if column not in exclude_column:
                df[column] = df[column].astype('float')
        return df
    except Exception as e:
        raise SensorException(e, sys)


def save_object(file_path:str,obj:object)->None:
    try:
        logging.info("Entered the save object method of MainUtils class")
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path,"wb") as file_obj:
            dill.dump(obj,file_obj)
        logging.info("Entered the save object method of MainUtils class")
    except Exception as e:
        raise SensorException(e, sys) from e

def load_object(file_path:str)->object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file : {file_path} is not exists")
        with open(file_path,"rb") as file_obj:
            return dill.load(file_obj)
    except Exception as e:
        raise SensorException(e, sys) from e

def save_numpy_arr_data(file_path:str,array:np.array):
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok=True)
        with open(file_path,'wb') as file_obj:
            np.save(file_obj,array)
    except Exception as e:
        raise SensorException(e, sys)


def load_numpy_arr_data(file_path:str)->np.array:
    try:
        with open(file_path,"rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise SensorException(e, sys)