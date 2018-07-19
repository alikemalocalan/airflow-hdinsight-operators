import airflow
from airflow import DAG

from hdinsight_operators import WebHCatHiveSubmitOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}

"""
Pre-run Steps:
1. Open the Airflow WebServer
2. Navigate to Admin -> Connections
3. Add a new connection
    1. Set the Conn Id as "livy_http_conn"
    2. Set the Conn Type as "http"
    3. Set the host
    4. Set the port (default for livy is 80)
    5. Save
"""

dag = DAG('hdinsinght',
          schedule_interval="@daily",  # Run once a day at midnight
          default_args=default_args)

hive_submit = WebHCatHiveSubmitOperator(task_id="hive-example-1",
                                        sql_file='wasb:///example/hivequery1.sql',
                                        job_name='hive-submit-task',
                                        cluster_name="hd-cluster",
                                        http_conn_id="livy_http_conn",
                                        dag=dag)
