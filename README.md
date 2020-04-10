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
Integrate Airflow with Datadog

Installation
All three steps below are needed for the Airflow integration to work properly. Before you begin, install the Datadog Agent version >=6.17 or >=7.17, which includes the StatsD/DogStatsD mapping feature.

Step 1: Configure Airflow to collect health metrics and service checks
Configure the Airflow check included in the Datadog Agent package to collect health metrics and service checks.

Edit the airflow.d/conf.yaml file, in the conf.d/ folder at the root of your Agent's configuration directory to start collecting your Airflow service checks. See the sample airflow.d/conf.yaml for all available configuration options.

Step 2: Connect Airflow to DogStatsD (included in the Datadog Agent) by using Airflow statsd feature to collect metrics
Install the Airflow StatsD plugin.

pip install 'apache-airflow[statsd]'
Update the Airflow configuration file airflow.cfg by adding the following configs:

[scheduler]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
Update the Datadog Agent main configuration file datadog.yaml by adding the following configs:

# dogstatsd_mapper_cache_size: 1000  # default to 1000
dogstatsd_mapper_profiles:
  - name: airflow
    prefix: "airflow."
    mappings:
      - match: "airflow.*_start"
        name: "airflow.job.start"
        tags:
          job_name: "$1"
      - match: "airflow.*_end"
        name: "airflow.job.end"
        tags:
          job_name: "$1"
      - match: "airflow.operator_failures_*"
        name: "airflow.operator_failures"
        tags:
          operator_name: "$1"
      - match: "airflow.operator_successes_*"
        name: "airflow.operator_successes"
        tags:
          operator_name: "$1"
      - match: 'airflow\.dag_processing\.last_runtime\.(.*)'
        match_type: "regex"
        name: "airflow.dag_processing.last_runtime"
        tags:
          dag_file: "$1"
      - match: 'airflow\.dag_processing\.last_run\.seconds_ago\.(.*)'
        match_type: "regex"
        name: "airflow.dag_processing.last_run.seconds_ago"
        tags:
          dag_file: "$1"
      - match: 'airflow\.dag\.loading-duration\.(.*)'
        match_type: "regex"
        name: "airflow.dag.loading_duration"
        tags:
          dag_file: "$1"
      - match: "airflow.pool.open_slots.*"
        name: "airflow.pool.open_slots"
        tags:
          pool_name: "$1"
      - match: "airflow.pool.used_slots.*"
        name: "airflow.pool.used_slots"
        tags:
          pool_name: "$1"
      - match: "airflow.pool.starving_tasks.*"
        name: "airflow.pool.starving_tasks"
        tags:
          pool_name: "$1"
      - match: 'airflow\.dagrun\.dependency-check\.(.*)'
        match_type: "regex"
        name: "airflow.dagrun.dependency_check"
        tags:
          dag_id: "$1"
      - match: 'airflow\.dag\.(.*)\.([^.]*)\.duration'
        match_type: "regex"
        name: "airflow.dag.task.duration"
        tags:
          dag_id: "$1"
          task_id: "$2"
      - match: 'airflow\.dag_processing\.last_duration\.(.*)'
        match_type: "regex"
        name: "airflow.dag_processing.last_duration"
        tags:
          dag_file: "$1"
      - match: 'airflow\.dagrun\.duration\.success\.(.*)'
        match_type: "regex"
        name: "airflow.dagrun.duration.success"
        tags:
          dag_id: "$1"
      - match: 'airflow\.dagrun\.duration\.failed\.(.*)'
        match_type: "regex"
        name: "airflow.dagrun.duration.failed"
        tags:
          dag_id: "$1"
      - match: 'airflow\.dagrun\.schedule_delay\.(.*)'
        match_type: "regex"
        name: "airflow.dagrun.schedule_delay"
        tags:
          dag_id: "$1"
      - match: 'airflow\.task_removed_from_dag\.(.*)'
        match_type: "regex"
        name: "airflow.dag.task_removed"
        tags:
          dag_id: "$1"
      - match: 'airflow\.task_restored_to_dag\.(.*)'
        match_type: "regex"
        name: "airflow.dag.task_restored"
        tags:
          dag_id: "$1"
      - match: "airflow.task_instance_created-*"
        name: "airflow.task.instance_created"
        tags:
          task_class: "$1"
Step 3: Restart Datadog Agent and Airflow
Restart the Agent.
Restart Airflow to start sending your Airflow metrics to the Agent DogStatsD endpoint.
Integration Service Checks
Use the default configuration of your airflow.d/conf.yaml file to activate the collection of your Airflow service checks. See the sample airflow.d/conf.yaml for all available configuration options.

Log collection
Available for Agent versions >6.0

Collecting logs is disabled by default in the Datadog Agent. Enable it in your datadog.yaml file:

logs_enabled: true
Uncomment and edit this configuration block at the bottom of your airflow.d/conf.yaml:

Change the path and service parameter values and configure them for your environment.

a. Configuration for DAG processor manager and Scheduler logs:

logs:
  - type: file
    path: '<PATH_TO_AIRFLOW>/logs/dag_processor_manager/dag_processor_manager.log'
    source: airflow
    service: '<SERVICE_NAME>'
    log_processing_rules:
      - type: multi_line
        name: new_log_start_with_date
        pattern: \[\d{4}\-\d{2}\-\d{2}
  - type: file
    path: '<PATH_TO_AIRFLOW>/logs/scheduler/*/*.log'
    source: airflow
    service: '<SERVICE_NAME>'
    log_processing_rules:
      - type: multi_line
        name: new_log_start_with_date
        pattern: \[\d{4}\-\d{2}\-\d{2}
Regular clean up is recommended for scheduler logs with daily log rotation.

b. Additional configuration for DAG tasks logs:

logs:
  - type: file
    path: '<PATH_TO_AIRFLOW>/logs/*/*/*/*.log'
    source: airflow
    service: '<SERVICE_NAME>'
    log_processing_rules:
      - type: multi_line
        name: new_log_start_with_date
        pattern: \[\d{4}\-\d{2}\-\d{2}
Caveat: By default Airflow uses this log file template for tasks: log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log. The number of log files will grow quickly if not cleaned regularly. This pattern is used by Airflow UI to display logs individually for each executed task.

If you do not view logs in Airflow UI, Datadog recommends this configuration in airflow.cfg: log_filename_template = dag_tasks.log. Then log rotate this file and use this configuration:

logs:
  - type: file
    path: '<PATH_TO_AIRFLOW>/logs/dag_tasks.log'
    source: airflow
    service: '<SERVICE_NAME>'
    log_processing_rules:
      - type: multi_line
        name: new_log_start_with_date
        pattern: \[\d{4}\-\d{2}\-\d{2}
Restart the Agent.



