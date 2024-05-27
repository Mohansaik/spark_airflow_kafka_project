from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from src.consumer import main





default_args={
    'owner':'mohan',
    'retries':2,
    'retry_delay': timedelta(seconds=30)
}


with DAG(dag_id="consumer_qutoes_dag",
         start_date=datetime(2024,4,18),
         schedule_interval='@daily',
         default_args=default_args) as dag:
    
    start_task =  BashOperator(task_id="start",
                               bash_command="echo started consumer task")
    
    python_task = PythonOperator(task_id="cosumer_qutoes_task",
                                 python_callable=main)
    
    end_task = BashOperator(task_id="end",
                            bash_command="echo successfully completed consumer qutoes task")
    
    start_task>>python_task>>end_task