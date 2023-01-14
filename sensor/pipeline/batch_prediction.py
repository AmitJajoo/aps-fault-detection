import os,sys
import pandas as pd
from sensor.exception import SensorException
from sensor.logger import logging
from datetime import datetime
from sensor.utils import load_object
from sensor.predictor import ModelResolver
import numpy as np
PREDICTION_DIR = "prediction"


def batch_prediction(input_file_path):
    try:
        os.makedirs(PREDICTION_DIR,exist_ok=True)
        logging.info(f"Reassing input file path {input_file_path}")
        model_resolver = ModelResolver()
        df = pd.read_csv(input_file_path)
        df.replace({"na":np.NAN},inplace=True)
        logging.info("Loading transformer to transform dataset")
        transformer = load_object(file_path = model_resolver.get_latest_transformer_path())

        input_feature_name = list(transformer.feature_names_in_)
        print(f"batch_prediction() input_feature_name -> {input_feature_name}")
        input_arr = transformer.transform(df[input_feature_name])
        logging.info(f"batch_prediction input_arr {df[input_feature_name]}")


        logging.info(f"Loading model to make prediction")
        model = load_object(file_path = model_resolver.get_latest_model_path())
        prediction = model.predict(input_arr)

        logging.info(f"Target encoder to convert predicted column into categorical")
        target_encoder = load_object(file_path = model_resolver.get_latest_target_encoder_path())

        categorical_prediction = target_encoder.inverse_transform(prediction)

        df['prediction'] = prediction
        df['categorical_prediction'] = categorical_prediction


        prediction_file_name = os.path.basename( input_file_path).replace(".csv",f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.csv")
        prediction_file_path = os.path.join(PREDICTION_DIR,prediction_file_name)
        print(f"batch_prediction prediction_file_path -> {prediction_file_path}")
        df.to_csv(prediction_file_path,index=False,header=True)
        return prediction_file_path
    except Exception as e:
        raise SensorException(e,sys)