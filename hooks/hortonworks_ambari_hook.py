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
from typing import Dict, Any
from urllib.parse import urlencode

from airflow.exceptions import AirflowConfigException
from airflow.hooks.http_hook import HttpHook


class HdpAmbariHook(HttpHook):

    def __init__(self, ambari_conn_id='hortonworks_ambari_default') -> None:
        """
        :param ambari_conn_id: connection id of Hortonworks Ambari cluster
                    which will be used to Hortonworks rest service
        :type ambari_conn_id: str
        """
        super().__init__(ambari_conn_id)

        connection = self.get_connection(ambari_conn_id)
        self.cluster_name = str(connection.host).split("//")[0].split(".")[0]  # clustername
        self.username = connection.login

        self.default_params = {"user.name": self.username}
        self.headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'X-Requested-By': self.username
        }
        self.hive_endpoint = "templeton/v1/"
        self.spark_endpoint = "livy/batches"

    def submit_hive_job(self, body_params: dict, arg: str = None) -> str:
        """
        Executes hql code or hive script in Azure HDInsight Cluster

        See https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Job

        :param arg: define arg params for hive =>  key1=value1;key2=value2
        :param execution_timeout: connection timeout of requesting to hortomwork cluster
        :type execution_timeout: int
        :param body_params: parametres of Hive script
        :type body_params: dict

        """

        if not ("file" in body_params or "execute" in body_params):
            raise AirflowConfigException("Request body must include file or execute params")

        body_params.update(self.default_params)

        if arg is not None:
            hive_defines = urlencode([("define", x) for x in str(arg).split(";")])
            self.query = urlencode(body_params) + "&" + hive_defines
        else:
            self.query = urlencode(body_params)

        self.method = "POST"
        submit_endpoint = self.hive_endpoint + "hive"

        self.log.debug("Submiting hive  Script: %s", str(self.query))
        response = self.run(endpoint=submit_endpoint, data=self.query, headers=self.headers)

        job_id = response["id"]
        self.log.info("Finished submitting hive script job_id: %s", job_id)
        return job_id

    def submit_spark_job(self, body_params: dict, ) -> str:
        """
        Submit spark job or spark-sql to Azure HDInsight Cluster

        :param execution_timeout: connection timeout of requesting to hortomwork cluster
        :type execution_timeout: int
        :param body_params: parametres of Spark Job
        :type body_params: dict
        """

        body_params.update(self.default_params)
        if not ("files" in body_params or "pyFiles" in body_params or "className" in body_params):
            raise AirflowConfigException("Request body must include files (and className) or pyFiles params")

        self.method = "POST"

        self.log.info("Submiting spark Job: %s ", json.dumps(body_params))
        response = self.run(endpoint=self.spark_endpoint,
                            data=json.dumps(body_params),
                            headers=self.headers)

        job_id = response["id"]
        self.log.debug("Finished submitting spark  job_id: %s", str(job_id))
        return job_id

    def _get_job_status(self, endpoint):
        self.method = "GET"
        return self.run(endpoint=endpoint,
                        data=self.default_params,
                        headers=self.headers)

    def get_spark_job_status(self, job_id) -> Dict[str, Any]:
        status_endpoint = self.spark_endpoint + "/" + str(job_id)
        return self._get_job_status(endpoint=status_endpoint)["state"]

    def get_hive_job_status(self, job_id) -> Dict[str, Any]:
        status_endpoint = self.hive_endpoint + "jobs/" + str(job_id)
        return self._get_job_status(endpoint=status_endpoint)["status"]
