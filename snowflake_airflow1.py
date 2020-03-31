import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    'owner':'airflow',
    'retries': 1,
    'retry_delay' : timedelta(minutes = 1),
    'email_on_failure' : True,
    'email_on_success' : True,
    'email_on_retry': True,
    'email' : ['tapas.das@datagrokr.com']
}

dag = DAG(
    dag_id='Snowflake_done',
    default_args = args,
    start_date = datetime(2020,3,30),
    schedule_interval = '0 12 * * *'
)

query_for_creation = [
    """create table public.tapas(emp_id int not null, f_name varchar(20), l_name varchar(20), salary int, domain varchar(20), qualification varchar(20));"""
    
]

query_for_loading = [
    """insert into public.tapas values('1','Tapas','Das',20000,'Data Engineer','MCA');""",
    """insert into public.tapas values('2','Sayan','Ghosh',45000,'Solution Architect','MTech');""",
    """insert into public.tapas values('3','Rahul','Saha',26000,'Web Developer','BSc');""",
    """insert into public.tapas values('4','Souvik','Laha',30000,'Android Developer','BCA');"""
]

# query_for_deletion = [
#     """QUERY FOR DELETION"""
# ]


def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_airflow")
    result = dwh_hook.get_first("select count(*) from public.tapas")
    logging.info("Number of rows in `public.tapas`  - %s", result[0])


with dag:
    create_table = SnowflakeOperator(
        task_id="snowflake_create",
        sql=query_for_creation,
        snowflake_conn_id="snowflake_airflow",
    )

with dag:
    load_table = SnowflakeOperator(
        task_id = 'snowflake_load',
        sql = query_for_loading,
        snowflake_conn_id = "snowflake_airflow"
    )

# with dag:
#     delete_data = SnowflakeOperator(
#         task_id = 'snowflake_delete',
#         sql = query_for_deletion,
#         snowflake_conn_id = "snowflake_airflow"
#     )

    get_count = PythonOperator(task_id = "get_count", python_callable = row_count)

create_table >> load_table >> get_count
