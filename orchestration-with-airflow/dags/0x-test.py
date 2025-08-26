from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id="spark_hello_world",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",   # only run manually
    catchup=False,
)
def spark_hello_world():

    spark_job = SparkSubmitOperator(
        task_id="hello_world_task",
        application="/Users/alexforest/Desktop/GithubProjects/ZTMDataEngineering/orchestration-with-airflow/spark_jobs/hello_world.py",  # <-- make sure this file exists
        name="hello_world_app",
        conn_id="spark_booking_2",
        conf={"spark.executorEnv.PYTHONPATH": "/Users/alexforest/Desktop/GithubProjects/ZTMDataEngineering/orchestration-with-airflow/venv/lib/python3.12/site-packages"},
        deploy_mode="client",   # so prints show in Airflow logs
        spark_binary="/Users/alexforest/Desktop/GitHubProjects/ZTMDataEngineering/orchestration-with-airflow/venv/bin/spark-submit",
        verbose=True
    )

    spark_job

dag_instance = spark_hello_world()