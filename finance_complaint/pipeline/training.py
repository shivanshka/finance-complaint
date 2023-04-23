from finance_complaint.components import DataIngestion, DataValidation, DataTransformation, ModelTrainer, ModelEvaluation
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
from finance_complaint.entity import (DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig, 
                                            DataTransformationConfig, ModelTrainerConfig, ModelEvaluationConfig)
from finance_complaint.entity import (DataIngestionArtifact, DataValidationArtifact, DataTransformationArtifact, 
                                        ModelTrainerArtifact, ModelEvaluationArtifact)
import os, sys


class TrainingPipeline:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config= training_pipeline_config

    def start_data_ingestion(self)-> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig(training_pipeline_config= self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config = data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact
        except Exception as e:
            raise FinanceException(e, sys)
    
    def start_data_validation(self, data_ingestion_artifact:DataIngestionArtifact)->DataValidationArtifact:
        try:
            data_validation_config = DataValidationConfig(training_pipeline_config=self.training_pipeline_config)
            data_validation = DataValidation(data_validation_config=data_validation_config, 
                                             data_ingestion_artifact=data_ingestion_artifact)
            data_validation_artifact = data_validation.initiate_data_validation()
            return data_validation_artifact
        except Exception as e:
            raise FinanceException(e, sys)
    
    def start_data_transformation(self, data_validation_artifact: DataValidationArtifact)-> DataTransformationArtifact:
        try:
            data_transformation_config = DataTransformationConfig(training_pipeline_config=self.training_pipeline_config)
            data_transformation = DataTransformation(data_validation_artifact = data_validation_artifact, 
                                                     data_transformation_config = data_transformation_config)
            data_transformation_artifact = data_transformation.initiate_data_transformation()
            return data_transformation_artifact
        except Exception as e:
            raise FinanceException(e, sys)

    def start_model_training(self, data_transformation_artifact: DataTransformationArtifact)-> ModelTrainerArtifact:
        try:
            model_trainer_config = ModelTrainerConfig(training_pipeline_config=self.training_pipeline_config)
            model_trainer = ModelTrainer(data_transformation_artifact = data_transformation_artifact, 
                                         model_trainer_config = model_trainer_config)
            model_trainer_artifact = model_trainer.initiate_model_training()
            return model_trainer_artifact
        except Exception as e:
            raise FinanceException(e, sys)

    def start_model_evaluation(self, data_validation_artifact: DataValidationArtifact, 
                                     model_trainer_artifact: ModelTrainerArtifact)-> ModelEvaluationArtifact:
        try:
            model_evaluation_config = ModelEvaluationConfig(training_pipeline_config=self.training_pipeline_config)
            model_evaluation = ModelEvaluation(data_validation_artifact = data_validation_artifact, 
                                               model_trainer_artifact = model_trainer_artifact, 
                                               model_eval_config = model_evaluation_config)      
            model_evaluation_artifact = model_evaluation.initiate_model_evaluation()
            return model_evaluation_artifact
        except Exception as e:
            raise FinanceException(e, sys)


    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            
            data_transformation_artifact = self.start_data_transformation(data_validation_artifact=data_validation_artifact)
            
            model_trainer_artifact = self.start_model_training(data_transformation_artifact=data_transformation_artifact)
            
            model_evaluation_artifact = self.start_model_evaluation(data_validation_artifact=data_validation_artifact, 
                                                                    model_trainer_artifact=model_trainer_artifact)
            
            if model_evaluation_artifact.model_accepted:
                pass
        except Exception as e:
            raise FinanceException(e, sys)