
from __future__ import annotations
from finance_complaint.pipeline import TrainingPipeline
from finance_complaint.entity import TrainingPipelineConfig
import json 
from textwrap import dedent
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    "clf_finance",
    default_args={"retries": 2},
    description="Consumer Financial Complaint",
    schedule="@weekly",
    start_date=pendulum.datetime(2023, 4, 26, tz="UTC"),
    catchup=False,
    tags=["machine_learning","classification","classification"],
) as dag:
    dag.doc_md = __doc__
    
    training_pipeline_config= TrainingPipelineConfig()
    training_pipeline = TrainingPipeline(training_pipeline_config=training_pipeline_config)

    def data_ingestion(**kwargs):
        ti = kwargs["ti"]
        data_ingestion_artifact = training_pipeline.start_data_ingestion()
        ti.xcom_push("data_ingestion_artifact", data_ingestion_artifact)


    def data_validation(**kwargs):
        from finance_complaint.entity import DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,\
        ModelTrainerArtifact,ModelEvaluationArtifact,ModelPusherArtifact,PartialModelTrainerRefArtifact,PartialModelTrainerMetricArtifact
        ti  = kwargs['ti']
        data_ingestion_artifact = ti.xcom_pull(task_ids="data_ingestion",key="data_ingestion_artifact")
        data_validation_artifact=training_pipeline.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
        ti.xcom_push('data_validation_artifact', data_validation_artifact)


    def data_transformation(**kwargs):
        from finance_complaint.entity import DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,\
        ModelTrainerArtifact,ModelEvaluationArtifact,ModelPusherArtifact,PartialModelTrainerRefArtifact,PartialModelTrainerMetricArtifact
        ti  = kwargs['ti']

        data_validation_artifact = ti.xcom_pull(task_ids="data_validation",key="data_validation_artifact")
        data_transformation_artifact=training_pipeline.start_data_transformation(
        data_validation_artifact=data_validation_artifact
        )
        ti.xcom_push('data_transformation_artifact', data_transformation_artifact)


    def model_trainer(**kwargs):
        from finance_complaint.entity import DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,\
        ModelTrainerArtifact,ModelEvaluationArtifact,ModelPusherArtifact,PartialModelTrainerRefArtifact,PartialModelTrainerMetricArtifact
        ti  = kwargs['ti']

        data_transformation_artifact = ti.xcom_pull(task_ids="data_transformation",key="data_transformation_artifact")
        model_trainer_artifact=training_pipeline.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
        ti.xcom_push('model_trainer_artifact', model_trainer_artifact)



    def model_evaluation(**kwargs):
        from finance_complaint.entity import DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,\
        ModelTrainerArtifact,ModelEvaluationArtifact,ModelPusherArtifact,PartialModelTrainerRefArtifact,PartialModelTrainerMetricArtifact
        ti  = kwargs['ti']

        data_validation_artifact = ti.xcom_pull(task_ids="data_validation",key="data_validation_artifact")

        model_trainer_artifact = ti.xcom_pull(task_ids="model_trainer",key="model_trainer_artifact")
        model_evaluation_artifact = training_pipeline.start_model_evaluation(data_validation_artifact=data_validation_artifact,
                                                                    model_trainer_artifact=model_trainer_artifact)

        ti.xcom_push('model_evaluation_artifact', model_evaluation_artifact)

    def push_model(**kwargs):
        ti  = kwargs['ti']
        model_evaluation_artifact = ti.xcom_pull(task_ids="model_evaluation",key="model_evaluation_artifact")
        model_trainer_artifact = ti.xcom_pull(task_ids="model_trainer",key="model_trainer_artifact")
        if model_evaluation_artifact.model_accepted:
            model_pusher_artifact = training_pipeline.start_model_pusher(model_trainer_artifact=model_trainer_artifact)
            print(f'Model pusher artifact: {model_pusher_artifact}')
        else:
            print("Trained model rejected.")
            print("Trained model rejected.")
        print("Training pipeline completed")

    def upload_data(**kwargs):
        import os
        bucket_name = os.getenv("BUCKET_NAME")
        os.system(f"aws s3 sync /app/artifact s3://{bucket_name}/artifact")
        os.system(f"aws s3 sync /app/saved_models s3://{bucket_name}/saved_models")


    data_ingestion_task = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion,
    )
    data_ingestion_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )

    data_validation_task = PythonOperator(
        task_id="data_validation",
        python_callable=data_validation,
    )
    data_validation_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )


    data_transformation_task = PythonOperator(
        task_id="data_transformation",
        python_callable=data_transformation,
    )
    data_transformation_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )

    model_trainer_task = PythonOperator(
        task_id="model_trainer",
        python_callable=model_trainer,
    )
    model_trainer_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )

    upload_data_task = PythonOperator(
        task_id="upload_data",
        python_callable=upload_data,
    )
    model_evaluation_task = PythonOperator(
        task_id="model_evaluation",
        python_callable=model_evaluation,
    )
    model_evaluation_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )

    push_model_task = PythonOperator(
        task_id="push_model",
        python_callable=push_model,
    )
    push_model_task.doc_md = dedent(
        """\
    #### Ingestion task
    This task created train and test file
    """
    )
    data_ingestion_task >> data_validation_task >>data_transformation_task >> model_trainer_task >>model_evaluation_task >> push_model_task >> upload_data_task