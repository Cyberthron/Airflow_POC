from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
    
}

# Program make a simple calculator

# This function adds two numbers 
def add(x, y):
   return x + y

# This function subtracts two numbers 
def sub(x, y):
   return x - y

# This function multiplies two numbers
def mul(x, y):
   return x * y

# This function divides two numbers
def div(x, y):
   return x / y

dag = DAG(
    dag_id="calculator", 
    default_args=default_args,
    start_date=datetime(2020,3,31),
    schedule_interval='*/12 * * * *',
    catchup=False
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

with dag:
    addition = PythonOperator(
        task_id="add",
        python_callable=add
    )

    subtraction = PythonOperator(
        task_id="sub", 
        python_callable=sub
    )
    multiplication = PythonOperator(
        task_id="mul",
        python_callable=mul
    )

    division = PythonOperator(
        task_id="div", 
        python_callable=div
    )
addition>>subtraction>>multiplication>>division

