import sys
from us_visa.exception.exception import VisaException
from us_visa.logging.logger import logging
from us_visa.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig, DataValidationConfig
from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidation


if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info("Initiating Data Ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)

        data_validation_config = DataValidationConfig(training_pipeline_config)
        data_validation = DataValidation(data_ingestion_artifact=data_ingestion_artifact, data_validation_config=data_validation_config)
        logging.info("Initiating Data Validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        print(data_validation_artifact)
    except Exception as e:
        raise VisaException(e,sys)

