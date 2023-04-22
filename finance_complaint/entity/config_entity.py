from dataclasses import dataclass
from finance_complaint.constant import *
from finance_complaint.entity.metadata_entity import DataIngestionMetadata
from finance_complaint.exception import FinanceException
import os, sys
from datetime import datetime


@dataclass
class TrainingPipelineConfig:
    pipeline_name:str = "artifact"
    artifact_dir:str = os.path.join(pipeline_name,TIMESTAMP)


class DataIngestionConfig:
    def __init__(self, training_pipeline_config:TrainingPipelineConfig,
                        from_date= DATA_INGESTION_MIN_START_DATE,
                        to_date=None):
        try:
            self.from_date = from_date
            min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d")
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")

            if from_date_obj < min_start_date:
                self.from_date = DATA_INGESTION_MIN_START_DATE

            if to_date is None:
                self.to_date = datetime.now().strftime("%Y-%m-%d")

            data_ingestion_master_dir = os.path.join(os.path.dirname(training_pipeline_config.artifact_dir), DATA_INGESTION_DIR)

            self.data_ingestion_dir = os.path.join(data_ingestion_master_dir, TIMESTAMP)
            self.metadata_file_path = os.path.join(data_ingestion_master_dir, DATA_INGESTION_METADATA_FILE_NAME)

            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path = self.metadata_file_path)

            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                self.from_date = metadata_info.to_date

            self.download_dir = os.path.join(self.data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR)
            self.failed_dir = os.path.join(self.data_ingestion_dir, DATA_INGESTION_FAILED_DIR)
            self.file_name = DATA_INGESTION_FILE_NAME

            self.feature_store_dir = os.path.join(data_ingestion_master_dir, DATA_INGESTION_FEATURE_STORE_DIR)
            self.datasource_url = DATA_INGESTION_DATA_SOURCE_URL
        
        except Exception as e:
            raise FinanceException(e, sys)

class DataValidationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig)-> None:
        try:
            data_validation_dir = os.path.join(training_pipeline_config.artifact_dir, DATA_VALIDATION_DIR)
            self.accepted_data_dir = os.path.join(data_validation_dir, DATA_VALIDATION_ACCEPTED_DATA_DIR)
            self.rejected_data_dir = os.path.join(data_validation_dir, DATA_VALIDATION_REJECTED_DATA_DIR)
            self.file_name = DATA_VALIDATION_FILE_NAME
        except Exception as e:
            raise FinanceException(e, sys)

class DataTransformationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig)-> None:
        try:
            data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir, DATA_TRANSFORMATION_DIR)
            self.export_pipeline_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_PIPELINE_DIR)
            self.tranformed_train_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_TRAIN_DIR)
            self.tranformed_test_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_TEST_DIR)
            self.file_name = DATA_TRANSFORMATION_FILE_NAME
            self.test_size = DATA_TRANSFORMATION_TEST_SIZE
        except Exception as e:
            raise FinanceException(e, sys)

class ModelTrainerConfig:
     def __init__(self, training_pipeline_config: TrainingPipelineConfig)-> None:
        try:
            model_trainer_dir = os.path.join(training_pipeline_config.artifact_dir, MODEL_TRAINER_DIR)
            self.trained_model_file_path = os.path.join(model_trainer_dir, 
                                                        MODEL_TRAINER_TRAINED_MODEL_DIR,
                                                        MODEL_TRAINER_MODEL_NAME)
            self.label_indexer_model_dir = os.path.join(model_trainer_dir, MODEL_TRAINER_LABEL_INDEXER_DIR)
            self.base_accuracy = MODEL_TRAINER_BASE_ACCURACY
            self.metric_list = MODEL_TRAINER_MODEL_METRIC_NAMES
        except Exception as e:
            raise FinanceException(e, sys)

