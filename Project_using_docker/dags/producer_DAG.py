from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime,timedelta
from src.producer import main




default_args={
    'owner':'mohan',
    'retries' :2,
    'retry_delay' : timedelta(seconds=30)

}


with DAG(dag_id='producer_qutoes_dag',
         start_date=datetime(2024,4,18),
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    start_task= BashOperator(task_id="start",
                             bash_command="echo started the producer_qutoes_dag")
    
    python_task= PythonOperator(task_id="qutoes_task",
                                python_callable=main)
    
    end_task = BashOperator(task_id="end",
                            bash_command="echo successfully executed the task")
    
    start_task>>python_task>>end_task





