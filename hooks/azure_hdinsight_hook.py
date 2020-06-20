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
import os

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from azure.common.client_factory import get_client_from_auth_file
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.mgmt.hdinsight.models import ClusterCreateProperties, ClusterCreateParametersExtended
from cached_property import cached_property
from msrestazure.azure_operation import AzureOperationPoller


class AzureHDInsightHook(BaseHook):

    def __init__(self, azure_conn_id='azure_default') -> None:
        super().__init__(azure_conn_id)

        self.conn_id = azure_conn_id
        connection = self.get_connection(azure_conn_id)
        extra_options = connection.extra_dejson

        self.client = self.get_conn
        self.resource_group_name = str(extra_options.get("resource_group_name"))
        self.resource_group_location = str(extra_options.get("resource_group_location"))

    @cached_property
    def get_conn(self):
        """
        Return a HDInsight client.

        This hook requires a service principal in order to work.
        After creating this service principal
        (Azure Active Directory/App Registrations), you need to fill in the
        client_id (Application ID) as login, the generated password as password,
        and tenantId and subscriptionId in the extra's field as a json.

        References
        https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
        https://docs.microsoft.com/en-us/python/api/overview/azure/key-vault?toc=%2Fpython%2Fazure%2FTOC.json&view=azure-python

        :return: HDInsight manage client
        :rtype: HDInsightManagementClient
        """
        conn = self.get_connection(self.conn_id)
        key_path = conn.extra_dejson.get('key_path', False)
        if key_path:
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(HDInsightManagementClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        if os.environ.get('AZURE_AUTH_LOCATION'):
            key_path = os.environ.get('AZURE_AUTH_LOCATION')
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(HDInsightManagementClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        credentials = ServicePrincipalCredentials(
            client_id=conn.login,
            secret=conn.password,
            tenant=conn.extra_dejson['tenantId']
        )

        subscription_id = conn.extra_dejson['subscriptionId']
        return HDInsightManagementClient(credentials, str(subscription_id))

    def create_cluster(self, cluster_params: ClusterCreateProperties, cluster_name):
        """
        References
        https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

        :return:
        """

        cluster_deployment: AzureOperationPoller = self.client.clusters.create(
            cluster_name=cluster_name,
            resource_group_name=self.resource_group_name,
            parameters=ClusterCreateParametersExtended(
                location=self.resource_group_location,
                tags={},
                properties=cluster_params
            ))
        cluster_deployment.wait()
        return cluster_deployment.result()

    def delete_cluster(self, cluster_name):
        """
        References
        https://docs.microsoft.com/en-us/python/api/overview/azure/hdinsight?view=azure-python

        :return:
        """
        delete_poller: AzureOperationPoller = self.client.clusters.delete(self.resource_group_name,
                                                                          cluster_name=cluster_name)
        delete_poller.wait()
        return delete_poller.result()
