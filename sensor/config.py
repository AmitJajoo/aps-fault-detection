from dataclasses import dataclass
import os
import pymongo

@dataclass
class EnvironmentVariable:
    get_mongo_db_url:str = os.getenv("MONGO_DB_URL")

env_var = EnvironmentVariable()
mongo_client = pymongo.MongoClient(env_var.get_mongo_db_url)