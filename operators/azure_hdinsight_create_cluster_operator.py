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
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.mgmt.hdinsight.models import ClusterCreateProperties

from hooks.azure_hdinsight_hook import AzureHDInsightHook


class AzureHDInsightCreateClusterOperator(BaseOperator):
    """
    See https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

    :param azure_conn_id: connection id of a service principal
            which will be used to delete Hdinsight cluster
    :type azure_conn_id: str
    :param cluster_name: cluster name of will  creating
    :type cluster_name: str
    """

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 cluster_params: ClusterCreateProperties,
                 azure_conn_id='azure_hdinsight_default',
                 *args,
                 **kwargs
                 ):
        super(AzureHDInsightCreateClusterOperator, self).__init__(*args, **kwargs)

        self.cluster_name = cluster_name
        self.cluster_params = cluster_params
        self.azure_conn_id = azure_conn_id

    def execute(self, context):
        azure_hook = AzureHDInsightHook(azure_conn_id=self.azure_conn_id)
        self.log.info("Executing HDInsightCreateClusterOperator ")
        azure_hook.create_cluster(self.cluster_params, self.cluster_name)
        self.log.info("Finished executing HDInsightCreateClusterOperator")
