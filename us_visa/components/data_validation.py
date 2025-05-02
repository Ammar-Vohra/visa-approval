import os, sys
import pandas as pd
from scipy.stats import ks_2samp
from us_visa.exception.exception import VisaException
from us_visa.logging.logger import logging
from us_visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from us_visa.entity.config_entity import DataValidationConfig
from us_visa.constants.training_pipeline import SCHEMA_FILE_PATH
from us_visa.utils.main_utils import read_yaml_file, write_yaml_file

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            logging.info("Initializing DataValidation...")
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
            logging.info("Schema config loaded successfully.")
        except Exception as e:
            raise VisaException(e, sys)

    @staticmethod
    def read_data(file_path):
        try:
            logging.info(f"Reading data from file: {file_path}")
            return pd.read_csv(file_path)
        except Exception as e:
            raise VisaException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame):
        try:
            logging.info("Validating number of columns in the dataframe...")
            expected_number_of_columns = len(self.schema_config["columns"])
            if len(dataframe.columns) == expected_number_of_columns:
                logging.info("Column count validation passed.")
                return True
            else:
                logging.warning("Column count validation failed.")
                return False
        except Exception as e:
            raise VisaException(e, sys)
        
    def validate_number_of_numerical_columns(self, dataframe: pd.DataFrame):
        try:
            logging.info("Validating number of numerical columns in the dataframe...")
            expected_number_of_num_columns = self.schema_config["numerical_columns"]
            actual_numerical_columns = [col for col in dataframe.columns if col in expected_number_of_num_columns]

            if len(actual_numerical_columns) == len(expected_number_of_num_columns):
                logging.info("Numerical column count validation passed.")
                return True
            else:
                logging.warning("Numerical column count validation failed.")
                return False
        except Exception as e:
            raise VisaException(e, sys)

    def validate_number_of_categorical_columns(self, dataframe: pd.DataFrame):
        try:
            logging.info("Validating number of categorical columns in the dataframe...")
            expected_number_of_cat_columns = self.schema_config["categorical_columns"]
            actual_number_of_cat_columns = [col for col in dataframe.columns if col in expected_number_of_cat_columns]

            if len(actual_number_of_cat_columns) == len(expected_number_of_cat_columns):
                logging.info("Categorical column count validation passed.")
                return True
            else:
                logging.warning("Categorical column count validation failed.")
                return False
        except Exception as e:
            raise VisaException(e, sys)

    def detect_data_drift(self, base_df, current_df, threshold=0.05):
        try:
            logging.info("Detecting data drift between train and test datasets...")
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_sample_dist = ks_2samp(d1, d2)

                if threshold <= is_sample_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False

                report.update({
                    column: {
                        "p_value": float(is_sample_dist.pvalue),
                        "drift_status": is_found
                    }
                })

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)

            write_yaml_file(file_path=drift_report_file_path, content=report)
            logging.info(f"Data drift report saved to: {drift_report_file_path}")
            return status
        except Exception as e:
            raise VisaException(e, sys)

    def initiate_data_validation(self):
        try:
            logging.info("Starting data validation process...")

            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            if not self.validate_number_of_columns(dataframe=train_dataframe):
                logging.warning("Train dataframe failed column validation.")
            else:
                logging.info("Train dataframe passed column validation.")

            if not self.validate_number_of_columns(dataframe=test_dataframe):
                logging.warning("Test dataframe failed column validation.")
            else:
                logging.info("Test dataframe passed column validation.")

            
            if not self.validate_number_of_numerical_columns(dataframe=train_dataframe):
                logging.warning("Train dataframe failed numerical column validation.")
            else:
                logging.info("Train dataframe passed numerical column validation.")

            if not self.validate_number_of_numerical_columns(dataframe=test_dataframe):
                logging.warning("Test dataframe failed numerical column validation.")
            else:
                logging.info("Test dataframe passed numerical column validation.")

            if not self.validate_number_of_categorical_columns(dataframe=train_dataframe):
                logging.warning("Train dataframe failed categorical column validation.")
            else:
                logging.info("Train dataframe passed categorical column validation.")

            if not self.validate_number_of_categorical_columns(dataframe=test_dataframe):
                logging.warning("Test dataframe failed categorical column validation.")
            else:
                logging.info("Test dataframe passed categorical column validation.")

            status = self.detect_data_drift(base_df=train_dataframe, current_df=test_dataframe)

            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_dataframe.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            logging.info(f"Validated train data saved to: {self.data_validation_config.valid_train_file_path}")

            test_dataframe.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)
            logging.info(f"Validated test data saved to: {self.data_validation_config.valid_test_file_path}")

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info("Data validation completed successfully.")
            return data_validation_artifact

        except Exception as e:
            raise VisaException(e, sys)
