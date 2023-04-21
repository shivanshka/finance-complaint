from collections import namedtuple
from finance_complaint.artifact_entity import DataIngestionArtifact
from finance_complaint.config_entity import DataIngestionConfig
from finance_complaint.metadata_entity import DataIngestionMetadata
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
import os, sys
from finance_complaint.config.spark_manager import spark_session
from datetime import datetime
import pandas as pd
import requests
import uuid
import json
import re
import time

DownloadUrl = namedtuple("DownloadUrl", ["url","file_path", "n_retry"])

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig, n_retry:int = 5,):
        try:
            logging.info(f"{'>>'*20} Starting Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_downloaded_urls: List[DownloadUrl] = []
            self.n_retry = n_retry
        
        except Exception as e:
            raise FinanceException(e, sys)

    def get_required_interval(self):

        start_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")
        n_diff_days = (end_date - start_date).days
        freq = None

        if n_diff_days >365:
            freq = "Y"
        elif n_diff_days > 30:
            freq = "M"
        elif n_diff_days > 7:
            freq = "W"

        logging.debug(f"{n_diff_days} hence freq: {freq}")

        if freq is None:
            intervals = pd.date_range(start=self.data_ingestion_config.from_date,
                                      end=self.data_ingestion_config.to_date,
                                      periods=2).astype('str').tolist()
        else:
            intervals = pd.date_range(start=self.data_ingestion_config.from_date,
                                      end=self.data_ingestion_config.to_date,
                                      periods=freq).astype('str').tolist()

        logging.debug(f"Prepared Interval: {intervals}")

        if self.data_ingestion_config.to_date not in intervals:
            intervals.append(self.data_ingestion_config.to_date)
        return intervals

    def download_files(self, n_day_interval_url: int = None):
        try:
            required_interval= self.get_required_interval()
            logging.info("Started downloading files")
            for index in range(1, len(required_interval)):
                from_date, to_date = required_interval[index-1], required_interval[index]
                logging.info(f"Generating data download url between {from_date} and {to_date}")
                datasource_url: str = self.data_ingestion_config.datasource_url

                url= datasource_url.replace('<todate>', to_date).replace('<fromdate>', from_date)
                logging.info(f"URL: {url}")

                file_name = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.data_ingestion_config.download_dir, file_name)
                download_url = DownloadUrl(url=url, file_path=file_path, n_retry=self.n_retry)
                self.download_data(download_url = download_url)
            logging.info(f"File download completed")

        except Exception as e:
            raise FinanceException(e, sys)

    def download_data(self, download_url: DownloadUrl):
        try:
            logging.info(f"Starting download operation: {download_url}")
            download_dir = os.path.join(download_url.file_path)

            # creating download directory
            os.makedirs(download_dir, exist_ok=True)

            # downloading data
            data = requests.get(download_url.url, params={'User-agent':f'your bot {uuid.uuid4()}'})

            try:
                logging.info(f"Started writing downloaded data into json files: {download_url.file_path}")
                
                #saving downloaded data into hard disk
                with open(download_url.file_path,"w") as file_obj:
                    finance_complaint_data = list(map(lambda x: x["_source"],
                                                        filter(lambda x: "_source" in x.keys(), 
                                                            json.loads(data.content))))
                    json.dump(finance_complaint_data, file_obj)
                logging.info(f"Downloaded data has been written into files: {download_url.file_path}")
            
            except Exception as e:
                logging.info("Failed to download hence retry again")

                #removing file failed 
                if os.path.join(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data, download_url=download_url)

        except Exception as e:
            logging.error(e)
            raise FinanceException(e, sys)

    def retry_download_data(self, data, download_url: DownloadUrl):
        try:
            if download_url.n_retry == 0:
                self.failed_downloaded_urls.append(download_url)
                logging.info(f"Unable to download file {download_url}")
                return
            
            content = data.content.decode("utf-8")
            wait_second = re.findall(r"\d+", content)

            if len(wait_second) > 0:
                time.sleep(int(wait_second[0]) + 2)

            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                            os.path.basename(download_url.file_path))
            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            download_url = DownloadUrl(url= download_url.url, 
                                       file_path=download_url.file_path, 
                                       n_retry=download_url.n_retry - 1)
            self.download_data(download_url=download_url)
        except Exception as e:
            raise FinanceException(e, sys)   

    def convert_files_to_parquet(self)-> str:
        try:
            json_data_dir = self.data_ingestion_config.download_dir
            data_dir = self.data_ingestion_config.feature_store_dir
            output_file_name = self.data_ingestion_config.file_name
            os.makedirs(data_dir, exist_ok=True)
            file_path = os.path.join(data_dir, f"{output_file_name}")
            
            logging.info(f"Parquet file will be created at: {file_path}")

            if not os.path.exists(json_data_dir):
                return file_path

            for file_name in os.listdir(json_data_dir):
                json_file_path = os.path.join(json_data_dir, file_name)
                logging.debug(f"Converting {json_file_path} into parquet format at {file_path}")

                df = spark_session.read.json(json_file_path)

                if df.count() > 0:
                    df.write.mode('append').parquet(file_path)
            return file_path
        except Exception as e:
            raise FinanceException(e, sys) 

    
    def write_metadata(self, file_path: str) -> None:
        try:
            logging.info("Writting metadata info into metadata file")
            metadata_info = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_info.write_metadata_info(from_date = self.data_ingestion_config.from_date,
                                              to_date = self.data_ingestion_config.to_date,
                                              data_file_path = file_path)

            logging.info("Metadata has been written")
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info(f"Started downloading json file")

            if self.data_ingestion_config.from_date != self.data_ingestion_config.to_date:
                self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                logging.info("Converting and combining downloaded json into parquet file")
                file_path = self.convert_files_to_parquet()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,
                                                    self.data_ingestion_config.file_name)
            artifact = DataIngestionArtifact(
                                    feature_store_file_path=feature_store_file_path,
                                    download_dir = self.data_ingestion_config.download_dir,
                                    metadata_file_path = self.data_ingestion_config.metadata_file_path)
            logging.info(f"Data Ingestion Artifact: {artifact}")
            return artifact
        except Exception as e:
            raise FinanceException(e, sys)
