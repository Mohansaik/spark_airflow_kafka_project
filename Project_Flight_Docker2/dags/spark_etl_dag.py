from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohan',
    'retry': 2,
    'retry_delay': timedelta(seconds=30)
}

with DAG(dag_id="spark_etl_dag",
         start_date=datetime(2024, 4, 18),
         default_args=default_args,
         schedule_interval='@daily') as dag:
    start = BashOperator(task_id="start",
                         bash_command="echo started the spark job ")

    spark_task = BashOperator(
        task_id='pyspark_consumer',
        bash_command='/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit '
                     '--master local[*] '
                     '--conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar" '
                     '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1,'
                     'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 '
                     '/usr/local/airflow/dags/src/etl.py ')

    start >> spark_task
