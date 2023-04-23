from dataclasses import dataclass
from datetime import datetime
import os

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

@dataclass
class EnvironmentVariables:
    mongo_db_url = os.getenv("MONGO_DB_URL")

env_var = EnvironmentVariables()


#Data Ingestion related variables
DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_files"
DATA_INGESTION_FILE_NAME = "finance_complaint"
DATA_INGESTION_FEATURE_STORE_DIR = "feature_store"
DATA_INGESTION_FAILED_DIR = "failed_downloaded_files"
DATA_INGESTION_METADATA_FILE_NAME = "meta_info.yaml"
DATA_INGESTION_MIN_START_DATE ='2022-07-01'
DATA_INGESTION_DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
                      f"?date_received_max=<todate>&date_received_min=<fromdate>" \
                      f"&field=all&format=json"

#Data Validation related variables
DATA_VALIDATION_DIR = "data_validation"
DATA_VALIDATION_FILE_NAME = 'finance_complaint'
DATA_VALIDATION_ACCEPTED_DATA_DIR = "accepted_data"
DATA_VALIDATION_REJECTED_DATA_DIR = "rejected_data"

#Data Transformation related variables
DATA_TRANSFORMATION_DIR = "data_transformation"
DATA_TRANSFORMATION_PIPELINE_DIR = "transformed_pipeline"
DATA_TRANSFORMATION_TRAIN_DIR = 'train'
DATA_TRANSFORMATION_TEST_DIR = 'test'
DATA_TRANSFORMATION_FILE_NAME ="finance_complaint"
DATA_TRANSFORMATION_TEST_SIZE = 0.3

# Model Training related variables
MODEL_TRAINER_DIR = "model_trainer"
MODEL_TRAINER_BASE_ACCURACY  = 0.6
MODEL_TRAINER_TRAINED_MODEL_DIR = "trained_model"
MODEL_TRAINER_MODEL_NAME = "finance_estimator"
MODEL_TRAINER_LABEL_INDEXER_DIR = "label_indexer"
MODEL_TRAINER_MODEL_METRIC_NAMES = ['f1',
                                    'weightedPrecision',
                                    'weightedRecall',
                                    'weightedTruePositiveRate',
                                    'weightedFalsePositiveRate',
                                    'weightedFMeasure',
                                    'truePositiveRateByLabel',
                                    'falsePositiveRateByLabel',
                                    'precisionByLabel',
                                    'recallByLabel',
                                    'fMeasureByLabel']

# Model Evaluation related variables
MODEL_SAVED_DIR = "saved_models"
MODEL_NAME = "finance_estimator"
MODEL_EVALUATION_DIR = "model_evaluation"
MODEL_EVALUATION_REPORT_DIR = "report"
MODEL_EVALUATION_REPORT_FILE_NAME=" evaluation_report"
MODEL_EVALUATION_THRESHOLD_VALUE=0.002
MODEL_EVALUATION_METRIC_NAMES = ['f1']

# Model Pusher related varaibles
MODEL_PUSHER_SAVED_MODEL_DIRS = 'saved_models'
MO
