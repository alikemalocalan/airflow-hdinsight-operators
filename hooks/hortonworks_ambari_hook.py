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
import json
import time

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class HdpAmbariHook(HttpHook):

    def __init__(self, ambari_conn_id='hortonworks_ambari_default'):
        """

        :param ambari_conn_id: connection id of Hortonworks Ambari cluster
                    which will be used to Hortonworks rest service
        :type ambari_conn_id: str
        """
        super(HdpAmbariHook, self).__init__(ambari_conn_id)

        self.non_terminated_status_list = ['INIT', 'starting', 'running', 'waiting',
                                           'available', 'not_started', 'busy', 'RUNNING', 'PREP']
        self.failed_status_list = ['error', 'cancelling', 'cancelled', 'shutting_down',
                                   'dead', 'killed', 'KILLED', 'FAILED']

        connection = self.get_connection(ambari_conn_id)
        self.cluster_name = str(connection.host).split("//")[0].split(".")[0]  # clustername
        self.username = connection.login

        self.default_params = {"user.name": self.username}
        self.headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'X-Requested-By': self.username
        }

    def submit_hive_job(self, body_params: dict, execution_timeout=5):
        """
        Executes hql code or hive script in Azure HDInsight Cluster

        See https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Job

        :param execution_timeout: connection timeout of requesting to hortomwork cluster
        :type execution_timeout: int
        :param body_params: parametres of Hive script
        :type body_params: dict

        """

        self.method = "POST"
        body_params.update(self.default_params)
        hive_endpoint = "templeton/v1/"
        submit_endpoint = hive_endpoint + "hive"

        self.log.debug("Submiting hive  Script: %s", str(body_params))
        response = self.run(endpoint=submit_endpoint, data=body_params, headers=self.headers)

        job_id = response["id"]
        status_endpoint = hive_endpoint + "jobs/" + str(job_id)
        self.log.info("Finished submitting hive script job_id: %s", job_id)

        statements_state = 'INIT'
        statements_exit_value = 'INIT'
        while statements_state in self.non_terminated_status_list:

            time.sleep(execution_timeout)
            try:
                result = self._get_job_status(endpoint=status_endpoint)["status"]
                statements_state = result["state"]
                statements_exit_value = result["exitValue"]
            except Exception:
                pass

            if statements_state is not None:
                # Check submitted job's result
                if statements_state in self.failed_status_list or statements_exit_value != "0":
                    result = "job failed. state: %s, exitValue: ", statements_state, statements_exit_value
                    self.log.error(result)
                    raise AirflowException(result)
                else:
                    self.log.debug("Checking Hive job: %s", statements_state)

        self.log.debug("Statement %s ", statements_state)
        self.log.debug("Finished executing WebHCatHiveSubmitOperator")

    def submit_spark_job(self, body_params: dict, execution_timeout=5):
        """
        Submit spark job or spark-sql to Azure HDInsight Cluster

        :param execution_timeout: connection timeout of requesting to hortomwork cluster
        :type execution_timeout: int
        :param body_params: parametres of Spark Job
        :type body_params: dict
        """

        spark_endpoint = "livy/batches"
        submit_endpoint = spark_endpoint
        self.method = "POST"
        body_params.update(self.default_params)

        self.log.info("Submiting spark Job: %s ", json.dumps(body_params))
        response = self.run(endpoint=submit_endpoint,
                            data=json.dumps(body_params),
                            headers=self.headers)

        job_id = response["id"]
        self.log.debug("Finished submitting spark  job_id: %s", str(job_id))

        statements_state = 'INIT'
        while statements_state in self.non_terminated_status_list:

            time.sleep(execution_timeout)
            try:
                status_endpoint = spark_endpoint + "/" + str(job_id)
                statements_state = self._get_job_status(endpoint=status_endpoint)["state"]
            except Exception:
                pass
            # Check submitted job's status
            if statements_state in self.failed_status_list:
                result = "job failed. state: %s", statements_state
                self.log.error(result)
                raise AirflowException(result)
            else:
                self.log.debug("Checking Spark job: %s", statements_state)

    def _get_job_status(self, endpoint):
        self.method = "GET"
        return self.run(endpoint=endpoint,
                        data=self.default_params,
                        headers=self.headers)
