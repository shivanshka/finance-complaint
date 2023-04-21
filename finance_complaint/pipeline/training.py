from finance_complaint.components import DataIngestion
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
from finance_complaint.entity import DataIngestionConfig, TrainingPipelineConfig
from finance_complaint.entity import DataIngestionArtifact
import os, sys


class TrainingPipeline:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config= training_pipeline_config

    def start_data_ingestion(self)-> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig(training_pipeline_config= self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config = data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        
        except Exception as e:
            raise FinanceException(e, sys)

    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise FinanceException(e, sys)