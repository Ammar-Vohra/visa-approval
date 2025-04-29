import os
import sys
import pymongo
import json
import certifi
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from us_visa.exception.exception import VisaException

load_dotenv()
ca = certifi.where()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
#print(MONGO_DB_URL)


class VisaDataExtraction:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise VisaException(e,sys)
        
    def csv_to_json_converter(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise VisaException(e,sys)
        
    def insert_data_to_mongodb(self, database, collection, records):
        try:
            self.database = database
            self.collection = collection
            self.records = records 

            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)

            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)

            return len(self.records)

        except Exception as e:
            raise VisaException(e,sys)

if __name__ == "__main__":
    FILE_PATH = "us_visa_data\Visadataset.csv"
    DATABASE = "AmmarVisa"
    COLLECTION = "VisaData"
    visa_obj = VisaDataExtraction()
    records = visa_obj.csv_to_json_converter(file_path=FILE_PATH)
    no_of_records = visa_obj.insert_data_to_mongodb(database=DATABASE, collection=COLLECTION, records=records)
    print(no_of_records)