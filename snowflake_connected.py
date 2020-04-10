import logging
import snowflake.connector			#for creating connection with snowflake
from datetime import datetime, timedelta	#Calculating dates
from airflow import DAG				#Creating DAG
from airflow.hooks.base_hook import BaseHook	# Integrate with snowflake
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from airflow.utils.email import send_email	#For email access
import statd
# Integrate with Datadogs
statsd_client = statsd.statsclient('localhost',8125,prefix='Airflow')

# Logging 

logging.basicConfig(level=logging.INFO)
user = logging.getLogger(__name__)

default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
    
}

# Snowflake information
# Information must be stored in connections
# It can be done with Airflow UI - 
# Admin -> Connections -> Create
database_name = 'TEST_DB'
table_name = 'customer'
schema_name = 'public'
snowflake_username = BaseHook.get_connection('snowflake').login 
snowflake_password = BaseHook.get_connection('snowflake').password	#Snowflake conn id=snowflake
snowflake_account = BaseHook.get_connection('snowflake').host

dag = DAG(
    dag_id="finally_done", 
    default_args=default_args,
    start_date=datetime(2020,3,31),
    schedule_interval='*/12 * * * *',
    catchup=False
)

#Loading Data

def load_data(**context):
    con = snowflake.connector.connect(user = snowflake_username, \
        password = snowflake_password, account = snowflake_account, \
        database=database_name, schema=schema_name)
    cs = con.cursor()

    load_table = "call loadable_data();"
    
    try:
        status = cs.execute(load_table).fetchall()[0][0]
        if status == 'Done.':
            user.info("Execution Done")
        else:
            user.error("Execution Failed".format(str(status)))
            raise AirflowException("Failed procedure run.")
    except Exception as ex:
        logger.error("Execution Failed".format(str(ex)))
        raise AirflowException("Execution Failed")
    finally:
        cs.close()

 #Delete Data

def drop_data(**context):
    con = snowflake.connector.connect(user = snowflake_username, \
        password = snowflake_password, account = snowflake_account,\
        database=database_name, schema=schema_name)
    cs = con.cursor()

    drop_table = "call dropmergetableProc();"
    cs.close()
    try:
        status = cs.execute(load_table).fetchall()[0][0]
        if status == 'Done.':
            user.info("Execution Done")
        else:
            user.error("Execution Failed".format(str(status)))
            raise AirflowException("Failed procedure run.")
    except Exception as ex:
        logger.error("Execution Failed".format(str(ex)))
        raise AirflowException("Execution Failed")
    finally:
        cs.close()
    
    
#Send Notification

def notify_email(contextDict, **kwargs):

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email('tapas.das@datagrokr.com', title, body) # Email_id for sending mail

with dag:
    insert_table = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    delete_table = PythonOperator(
        task_id="drop_data", 
        python_callable=drop_data
    )

insert_table >> delete_table
