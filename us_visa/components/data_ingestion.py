import os
import sys
import pymongo
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from us_visa.exception.exception import VisaException
from us_visa.logging.logger import logging
from us_visa.entity.config_entity import DataIngestionConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

class DataIngestion:
    def __init__(self, data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
            logging.info("DataIngestionConfig initialized.")
        except Exception as e:
            raise VisaException(e,sys)

    def export_data_from_mongodb(self):
        try:
            logging.info("Starting data export from MongoDB.")
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))
            logging.info(f"Data fetched from MongoDB collection: {collection_name} with shape {df.shape}")

            if "_id" in df.columns.to_list():
                df.drop(columns=["_id"], axis=1, inplace=True)
                logging.info("Dropped '_id' column from DataFrame.")


            df.replace({"na": np.nan}, inplace=True)
            logging.info("Replaced 'na' strings with np.nan.")
            return df 
        
        except Exception as e:
            raise VisaException(e,sys)
        
    def export_data_to_feature_store(self, dataframe:pd.DataFrame):
        try:
            logging.info("Exporting data to feature store.")
            feature_store_file_path = self.data_ingestion_config.feature_store_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            logging.info(f"Data exported to feature store at path: {feature_store_file_path}")
            return dataframe
        except Exception as e:
            raise VisaException(e,sys)
        
    def data_split_ratio(self, dataframe:pd.DataFrame):
        try:
            logging.info("Splitting data into train and test sets.")
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info(f"Train set shape: {train_set.shape}, Test set shape: {test_set.shape}")

            dir_path = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_set.to_csv(
                self.data_ingestion_config.train_file_path, index=False, header=True
            )
            test_set.to_csv(
                self.data_ingestion_config.test_file_path, index=False, header=True
            )
            logging.info("Train and test sets saved to respective paths.")
        except Exception as e:
            raise VisaException(e,sys)
        
    def initiate_data_ingestion(self):
        try:
            logging.info("Initiating data ingestion pipeline.")
            dataframe = self.export_data_from_mongodb()
            dataframe = self.export_data_to_feature_store(dataframe)
            self.data_split_ratio(dataframe)

            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=self.data_ingestion_config.train_file_path,
                test_file_path=self.data_ingestion_config.test_file_path
            )
            logging.info("Data ingestion completed successfully.")
            return data_ingestion_artifact
        except Exception as e: 
            raise VisaException(e,sys)
