import pandas as pd
from sensor.config import mongo_client
import os,sys
from sensor.exception import SensorException
from sensor.logger import logging

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