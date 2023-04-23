from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
from finance_complaint.entity import ModelPusherConfig
from finance_complaint.entity import ModelTrainerArtifact, ModelPusherArtifact
from finance_complaint.ml.estimator import ModelResolver
from pyspark.ml.pipeline import PipelineModel
import os, sys


class ModelPusher:
    def __init__(self, model_trainer_artifact: ModelTrainerArtifact,
                        model_pusher_config: ModelPusherConfig):
        try:
            self.model_trainer_artifact = model_trainer_artifact
            self.model_pusher_config = model_pusher_config
            self.model_resolver = ModelResolver(model_dir=self.model_pusher_config.saved_model_dir)
        except Exception as e:
            raise FinanceException(e, sys)

    def push_model(self)->str:
        try:
            trained_model_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
            saved_model_path = self.model_resolver.get_save_model_path
            model = PipelineModel.load(trained_model_path)
            model.save(saved_model_path)
            model.save(self.model_pusher_config.pusher_model_dir)
            return saved_model_path
        except Exception as e:
            raise FinanceException(e, sys)  

    def initiate_model_pusher(self)-> ModelPusherArtifact:
        try:
            saved_model_dir = self.push_model()
            model_pusher_artifact = ModelPusherArtifact(model_pusher_dir=self.model_pusher_config.pusher_model_dir,
                                                        saved_model_dir=saved_model_dir)
            logging.info(f"Model pusher artifact: {model_pusher_artifact}")
            return model_pusher_artifact
        except Exception as e:
            raise FinanceException(e, sys)  
