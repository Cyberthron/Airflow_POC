# Airflow_POC

export AIRFLOW_HOME=~/airflow

# install from pypi using pip
pip install apache-airflow

# initialize the database
airflow initdb

pip install snowflake-sqlalchemy

Creating Connection

Conn Id: <CONNECTION_ID>
Conn Type: Snowflake
Host: <YOUR_SNOWFLAKE_HOSTNAME>
Schema: <YOUR_SNOWFLAKE_SCHEMA>
Login: <YOUR_SNOWFLAKE_USERNAME>
Password: <YOUR_SNOWFLAKE_PASSWORD>
Extra: {
        "account": <YOUR_SNOWFLAKE_ACCOUNT_NAME>,
        "warehouse": <YOUR_SNOWFLAKE_WAREHOUSE_NAME>,
         "database": <YOUR_SNOWFLAKE_DB_NAME>,
         "region": <YOUR_SNOWFLAKE_HOSTED_REGION>
    }
