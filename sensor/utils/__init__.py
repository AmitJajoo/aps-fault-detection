import pandas as pd
from sensor.config import mongo_client
import os,sys
from sensor.exception import SensorException
from sensor.logger import logging
import yaml
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
    for column in df.columns:
        if column not in exclude_column:
            df[column] = df[column].astype('float')
    return df
