import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from hdinsight_operators import WebHCatHiveSubmitOperator, LivySparkSubmitOperator, HDInsightCreateClusterOperator, \
    HDInsightDeleteClusterOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}

client_id = Variable.get('AZURE_CLIENT_ID')
secret = Variable.get('AZURE_CLIENT_SECRET')
tenant = Variable.get('AZURE_TENANT_ID')
subscription_id = Variable.get('AZURE_SUBSCRIPTION_ID')

cluster_name = ""
cluster_username = ""
cluster_password = ""
resource_group_name = ""
resource_group_location = ""
deploy_name = ""

# inside template fıle from Azure resource deploy service
template_file = ""
# inside parameters  fıle from Azure resource deploy service
parameter_file = ""

dag = DAG('hdinsinght_test-dag',
          schedule_interval="@daily",  # Run once a day at midnight
          default_args=default_args)

start_cluster = HDInsightCreateClusterOperator(task_id="start_cluster",
                                               client_id=client_id,
                                               secret=secret,
                                               tenant=tenant,
                                               subscription_id=subscription_id,
                                               resource_group_name=resource_group_name,
                                               resource_group_location=resource_group_location,
                                               deploy_name=deploy_name,
                                               template_json=template_file,
                                               parameters_json=parameter_file,
                                               dag=dag,
                                               trigger_rule=TriggerRule.ALL_SUCCESS)

hive_submit = WebHCatHiveSubmitOperator(task_id="hive-example-1",
                                        sql_file='wasb:///example/hivequery2.sql',
                                        # statusdir='wasb:///example/status_hive',
                                        arg="hive_execution_engine=tez;hive_db_name=default",
                                        cluster_name="hd-cluster",
                                        trigger_rule=TriggerRule.ALL_SUCCESS,
                                        dag=dag)

spark_submit = LivySparkSubmitOperator(task_id="spark-example-1",
                                       application_file="wasb:///jars/calculatepi-1.0-SNAPSHOT-dep.jar",
                                       main_class="org.alikemal.spark.examples.SparkPi",
                                       name="spark-task-2",
                                       dag=dag)

delete_cluster = HDInsightDeleteClusterOperator(task_id="delete_cluster",
                                                client_id=client_id,
                                                secret=secret,
                                                tenant=tenant,
                                                subscription_id=subscription_id,
                                                trigger_rule=TriggerRule.ALL_DONE,
                                                resource_group_name=resource_group_name,
                                                resource_group_location=resource_group_location,
                                                cluster_name=cluster_name,
                                                dag=dag)
