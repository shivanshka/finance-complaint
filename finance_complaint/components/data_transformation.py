from finance_complaint.entity import FinanceDataSchema
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging
from finance_complaint.entity import DataTransformationConfig
from finance_complaint.entity import DataValidationArtifact, DataTransformationArtifact
from finance_complaint.ml.feature import FrequencyImputer, DerivedFeatureGenerator, FrequencyEncoder

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rand
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import (StandardScaler, VectorAssembler, OneHotEncoder, 
                                        StringIndexer, Imputer, IDF, Tokenizer, HashingTF)
import os, sys


class DataTransformation:

    def __init__(self, data_validation_artifact: DataValidationArtifact,
                        data_transformation_config: DataTransformationConfig,
                        schema = FinanceDataSchema()):
        try:
            self.data_val_artifact = data_validation_artifact
            self.data_tf_config = data_transformation_config
            self.schema = schema
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self)->DataFrame:
        try:
            file_path = self.data_val_artifact.accepted_file_path
            dataframe: DataFrame = spark_session.read.parquet(file_path)
            dataframe.printSchema()
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def get_data_transformation_pipeline(self)-> Pipeline:
        try:
            stages=[]
            derived_feature = DerivedFeatureGenerator(inputCols=self.schema.derived_input_features,
                                                      outputCols=self.schema.derived_output_features)
            stages.append(derived_feature)

            imputer = Imputer(inputCols=self.schema.numerical_features, outputCols=self.schema.im_numerical_features)
            stages.append(imputer)

            frequency_imputer = FrequencyImputer(inputCols=self.schema.one_hot_encoding_features, 
                                                 outputCols=self.schema.im_one_hot_encoding_features)
            stages.append(frequency_imputer)

            for im_one_hot_feature, string_indexer_col in zip(self.schema.im_one_hot_encoding_features, 
                                                              self.schema.string_indexer_one_hot_features):
                string_indexer = StringIndexer(inputCol=im_one_hot_feature, outputCol=string_indexer_col)
                stages.append(string_indexer)

            one_hot_encoder = OneHotEncoder(inputCols=self.schema.string_indexer_one_hot_features,
                                            outputCols=self.schema.tf_one_hot_encoding_features)
            stages.append(one_hot_encoder)

            tokenizer = Tokenizer(inputCol=self.schema.tfidf_features[0], outputCol="words")
            stages.append(tokenizer)

            hashing_tf = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawfeatures", numFeatures=40)
            stages.append(hashing_tf)

            idf = IDF(inputCol=hashing_tf.getOutputCol(), outputCol=self.schema.tf_tfidf_features[0])
            stages.append(idf)

            vector_assembler = VectorAssembler(inputCols=self.schema.input_features, 
                                               outputCol=self.schema.vector_assembler_output)
            stages.append(vector_assembler)

            standard_scalar = StandardScaler(inputCol=self.schema.vector_assembler_output,
                                             outputCol=self.schema.scaled_vector_input_features)
            stages.append(standard_scalar)

            pipeline = Pipeline(stages=stages)

            logging.info(f"Data Transformation pipeline: [{pipeline}]")
            print(pipeline.stages)
            return pipeline

        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info(f"{'>>'*20} Data Transformation Started {'<<'*20}")
            dataframe: DataFrame = self.read_data()

            logging.info(f"Number of row: [{dataframe.count()}] and column: [{len(dataframe.columns)}]")

            test_size = self.data_tf_config.test_size

            logging.info(f"Splitting dataset into train and test set using ratio: {1-test_size}:{test_size}")
            train_dataframe, test_dataframe = dataframe.randomSplit([1-test_size, test_size])

            logging.info(f"Train dataset has number of row: [{train_dataframe.count()}] and"
                                   f" column: [{len(train_dataframe.columns)}]")
            logging.info(f"Test dataset has number of row: [{test_dataframe.count()}] and"
                                   f" column: [{len(test_dataframe.columns)}]")

            pipeline = self.get_data_transformation_pipeline()
            transformed_pipeline = pipeline.fit(train_dataframe)
            required_columns = [self.schema.scaled_vector_input_features, self.schema.target_column]

            transformed_trained_dataframe = transformed_pipeline.transform(train_dataframe)
            transformed_trained_dataframe = transformed_trained_dataframe.select(required_columns)

            transformed_test_dataframe = transformed_pipeline.transform(test_dataframe)
            transformed_test_dataframe = transformed_test_dataframe.select(required_columns)

            export_pipeline_file_path = self.data_tf_config.export_pipeline_dir
            os.makedirs(export_pipeline_file_path, exist_ok=True)
            os.makedirs(self.data_tf_config.tranformed_train_dir, exist_ok=True)
            os.makedirs(self.data_tf_config.tranformed_test_dir, exist_ok=True)

            transformed_train_data_file_path = os.path.join(self.data_tf_config.tranformed_train_dir,
                                                            self.data_tf_config.file_name)
            transformed_test_data_file_path = os.path.join(self.data_tf_config.tranformed_test_dir,
                                                            self.data_tf_config.file_name)
            
            logging.info(f"Saving transformation pipeline at: [{export_pipeline_file_path}]")
            transformed_pipeline.save(export_pipeline_file_path)   
            
            logging.info(f"Saving transformed train data at: [{transformed_train_data_file_path}]")
            print(transformed_trained_dataframe.count(), len(transformed_trained_dataframe.columns))
            transformed_trained_dataframe.write.parquet(transformed_train_data_file_path)

            logging.info(f"Saving transformed test data at: [{transformed_test_data_file_path}]")     
            print(transformed_test_dataframe.count(), len(transformed_test_dataframe.columns))
            transformed_test_dataframe.write.parquet(transformed_test_data_file_path) 

            data_tf_artifact = DataTransformationArtifact(
                                        transformed_train_file_path=transformed_train_data_file_path, 
                                        transformed_test_file_path=transformed_test_data_file_path, 
                                        exported_pipeline_file_path=export_pipeline_file_path)                                          
            
            logging.info(f"Data Transformation Artifact: [{data_tf_artifact}]")
            return data_tf_artifact

        except Exception as e:
            raise FinanceException(e, sys)


