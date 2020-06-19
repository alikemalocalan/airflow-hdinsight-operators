# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from datetime import timedelta

from airflow import DAG
from airflow.utils import dates
from airflow.utils.trigger_rule import TriggerRule
from azure.mgmt.hdinsight.models import ClusterCreateProperties, \
    OSType, Tier, ClusterDefinition, ComputeProfile, Role, \
    HardwareProfile, LinuxOperatingSystemProfile, OsProfile, \
    StorageProfile, StorageAccount

from operators.azure_hdinsight_create_cluster_operator \
    import AzureHDInsightCreateClusterOperator
from operators.azure_hdinsight_delete_cluster_operator \
    import AzureHDInsightDeleteClusterOperator
from operators.azure_livysparksubmit_operator \
    import AzureLivySparkSubmitOperator
from operators.azure_webhcathivesubmit_operator \
    import AzureWebHCatHiveSubmitOperator

"""
Create azure_default connection  for cluster creating

Exanmple:

Firstly
    Set an environment variable for AZURE_AUTH_LOCATION:
    export AZURE_AUTH_LOCATION=~/.azure/azure_credentials.json
References:
    docs.microsoft.com/en-us/python/azure/python-sdk-azure-authenticate?view=azure-python

Conn Id: azure_default
Conn Type : None
Host   : None
Schema : None
Login  : azure client_id
Password  : azure client_secret
Port   : None

Extra : {
  "key_path": "~/.azure/azure_credentials.json",
  "subscriptionId": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
  "tenantId": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
  "resource_group_name": "example_rg",
  "resource_group_location": "example_rg_location"
}

References:
    docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal


After Create Hortonworks Ambari connection  for submitting jobs

Conn Id: hortonworks_ambari_default
Conn Type : None
Host   : https://cluster_name.azurehdinsight.net
Schema : https
Login  : Cluster user name
Password  : cluster password
Port   : None (or Optional)

"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

example_cluster_name = "test-cluster"

example_cluster_params: ClusterCreateProperties = ClusterCreateProperties(
    cluster_version="3.6",
    os_type=OSType.linux,
    tier=Tier.standard,
    cluster_definition=ClusterDefinition(
        kind="spark",
        configurations={
            "gateway": {
                "restAuthCredential.enabled_credential": "True",
                "restAuthCredential.username": "username",
                "restAuthCredential.password": "password"
            }
        }
    ),
    compute_profile=ComputeProfile(
        roles=[
            Role(
                name="headnode",
                target_instance_count=2,
                hardware_profile=HardwareProfile(vm_size="Large"),
                os_profile=OsProfile(
                    linux_operating_system_profile=LinuxOperatingSystemProfile(
                        username="username",
                        password="password"
                    )
                )
            ),
            Role(
                name="workernode",
                target_instance_count=1,
                hardware_profile=HardwareProfile(vm_size="Large"),
                os_profile=OsProfile(
                    linux_operating_system_profile=LinuxOperatingSystemProfile(
                        username="username",
                        password="password"
                    )
                )
            )
        ]
    ),
    storage_profile=StorageProfile(
        storageaccounts=[StorageAccount(
            name="storage_account",
            key="storage_account_key",
            container="container",
            is_default=True
        )]
    )
)

dag = DAG(dag_id='azure_hdinsinght_test_dag',
          schedule_interval=timedelta(1),
          default_args=default_args,
          tags=['example']
          )

start_cluster = AzureHDInsightCreateClusterOperator(task_id="start_cluster",
                                                    cluster_name=example_cluster_name,
                                                    cluster_params=example_cluster_params,
                                                    trigger_rule=TriggerRule.ALL_SUCCESS,
                                                    dag=dag)

hive_submit = AzureWebHCatHiveSubmitOperator(task_id="hive-example",
                                             file='wasb:///example/hivequery.sql',
                                             # statusdir='wasb:///example/status_hive',
                                             arg="hive_execution_engine=tez;hive_db_name=default",
                                             trigger_rule=TriggerRule.ALL_SUCCESS,
                                             dag=dag)

spark_submit = AzureLivySparkSubmitOperator(task_id="spark-example-pi",
                                            application_file="wasb:///jars/calculatepi-1.0-SNAPSHOT-dep.jar",
                                            main_class="org.apache.spark.examples.SparkPi",
                                            name="spark-task-1",
                                            trigger_rule=TriggerRule.ALL_SUCCESS,
                                            dag=dag)

delete_cluster = AzureHDInsightDeleteClusterOperator(task_id="delete_cluster",
                                                     cluster_name=example_cluster_name,
                                                     dag=dag)

start_cluster >> hive_submit >> spark_submit >> delete_cluster
