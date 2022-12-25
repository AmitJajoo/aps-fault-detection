import logging
import os
import time
from datetime import datetime

#log file name
LOF_FILE_NAME =  f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"

#log directory
LOG_FILE_DIRECTORY = os.path.join(os.getcwd(),"logs")

#create folder if not available
os.makedirs(LOG_FILE_DIRECTORY,exist_ok=True)