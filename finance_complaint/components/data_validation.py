from finance_complaint.entity import DataIngestionArtifact, DataValidationArtifact
from finance_complaint.entity import DataValidationConfig
from finance_complaint.entity import FinanceDataSchema
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging

from pyspark.sql.functions import lit, col
from pyspark.sql import DataFrame

import os, sys
from collections import namedtuple
from typing import List, Dict

MissingReport = namedtuple('MissingReport', ["total_row", "missing_row", "missing_percentage"])
ERROR_MESSAGE = "error_msg"

class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig,
                       data_ingestion_artifact: DataIngestionArtifact,
                       schema=FinanceDataSchema()):
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema = schema
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self)->DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                                        self.data_ingestion_artifact.feature_store_file_path).limit(10000)
            logging.info(f"Dataframe is created using file: {self.data_ingestion_artifact.feature_store_file_path}")
            logging.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)    

    @staticmethod
    def get_missing_report(dataframe: DataFrame)-> Dict[str, MissingReport]:
        try:
            missing_report: Dict[str:MissingReport] = dict()
            logging.info("Preparing missing report for each column")
            number_of_row = dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(total_row = number_of_row,
                                                        missing_row=missing_row,
                                                        missing_percentage=missing_percentage)
            logging.info(f"Missing report prepared: {missing_report}")  
            return missing_report
        except Exception as e:
            raise FinanceException(e, sys) 

    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float=0.2)-> List[str]:
        try:
            missing_report: Dict[str, MissingReport] = self.get_missing_report(dataframe=dataframe)

            unwanted_column: List[str] = self.schema.unwanted_columns
            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
            unwanted_column = list(set(unwanted_column))
            return unwanted_column
        except Exception as e:
            raise FinanceException(e, sys)

    def drop_unwanted_columns(self, dataframe: DataFrame)->DataFrame:
        try:
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe)
            logging.info(f"Dropping feature: {','.join(unwanted_columns)}")
            unwanted_dataframe: DataFrame = dataframe.select(unwanted_columns)

            unwanted_dataframe = unwanted_dataframe.withColumn(ERROR_MESSAGE, lit("Contains many missing values"))
            rejected_dir = os.path.join(self.data_validation_config.rejected_data_dir, "missing_data")
            os.makedirs(rejected_dir, exist_ok=True)

            file_path= os.path.join(rejected_dir, self.data_validation_config.file_name)

            logging.info(f"Writting dropped column into file: [{file_path}]")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe: DataFrame = dataframe.drop(*unwanted_columns)
            logging.info(f"Remaining number of columns: [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(filter(lambda x: x in self.schema.required_columns, dataframe.columns))

            if len(columns) != len(self.schema.required_columns):
                raise Exception(f"Required columns missing\n Expected columns: {self.schema.required_columns}\n\
                    Found Columns: {columns}")
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logging.info(f"Initiating data validation")
            dataframe: DataFrame = self.read_data()

            logging.info("Dropping Unwanted Columns")
            dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            self.is_required_columns_exist(dataframe=dataframe)
            logging.info(f"Saving validated data")
            print(f"Row: [{dataframe.count()}] Columns: [{len(dataframe.columns)}]")
            print(f"Expected Columns: [{self.schema.required_columns}]\nPresent Columns: [{dataframe.columns}]")

            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(self.data_validation_config.accepted_data_dir,
                                                self.data_validation_config.file_name)

            dataframe.write.parquet(accepted_file_path)

            data_validation_artifact = DataValidationArtifact(accepted_file_path=accepted_file_path,
                                                              rejected_dir=self.data_validation_config.rejected_data_dir)
            logging.info(f"Data Validation artifact: [{data_validation_artifact}]")
            return data_validation_artifact
        except Exception as e:
            raise FinanceException(e, sys)
