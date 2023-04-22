import yaml
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
import os, sys
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame


def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(e, sys) from e

def write_yaml_file(file_path:str, data:dict=None):
    try:
        os.makedirs((os.path.dirname(file_path)), exist_ok=True)
        with open(file_path, "w") as yaml_file:
            yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e, sys) from e

def get_score(dataframe: DataFrame, metric_name, label_col, prediction_col) ->float:
    try:
        evaluator = MulticlassClassificationEvaluator(labelCol=label_col, 
                                                      predictionCol=prediction_col, 
                                                      metricName=metric_name)
        score = evaluator.evaluate(dataframe)
        print(f"{metric_name} score: {score}")
        logging.info(f"{metric_name} score: {score}")
        return score

    except Exception as e:
        raise FinanceException(e, sys) from e   