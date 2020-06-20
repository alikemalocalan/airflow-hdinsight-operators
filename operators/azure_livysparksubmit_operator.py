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
from airflow.exceptions import AirflowConfigException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.hortonworks_ambari_hook import HdpAmbariHook


class AzureLivySparkSubmitOperator(BaseOperator):
    """
    Submit and Start a Spark Job on a Azure HDInsight cluster.

    See http://livy.incubator.apache.org/docs/latest/rest-api.html

    :param ambari_conn_id: connection id of Azure HDInsight cluster
            which will be used to Hortonworks rest service
    :type ambari_conn_id: str
    :param main_class: main class to be submitted job
    :type main_class: str
    :param files:	files to be used in this session
    :type files:	list[Str]
    :param jars:	jars to be used in this session
    :type jars:	list[str]
    :param pyFiles:	Python files to be used in this session
    :type pyFiles:	list[str]
    :param driverMemory:	Amount of memory to use for the driver process
    :type driverMemory:	str
    :param driverCores:	Number of cores to use for the driver process
    :type driverCores:	int
    :param executorMemory:	Amount of memory to use per executor process
    :type executorMemory:	str
    :param executorCores:	Number of cores to use for each executor
    :type executorCores:	int
    :param numExecutors:	Number of executors to launch for this session
    :type numExecutors:	int
    :param archives:	Archives to be used in this session
    :type archives:	str
    :param queue:	The name of the YARN queue to which submitted
    :type queue:	str
    :param name:	The name of this session
    :type name:	str
    :param conf:	Spark configuration properties
    :type conf:	dict
    """

    @apply_defaults
    def __init__(self,
                 ambari_conn_id='hortonworks_ambari_default',
                 application_file=None,
                 main_class=None,
                 master=None,
                 name=None,
                 jars=None,
                 py_files=None,
                 files=None,
                 executor_memory=None,
                 executor_cores=None,
                 num_executors=None,
                 driver_memory=None,
                 driver_cores=None,
                 archives=None,
                 queue=None,
                 application_args=None,
                 conf=None,
                 do_xcom_push: bool = True,
                 *args,
                 **kwargs
                 ):

        super(AzureLivySparkSubmitOperator, self).__init__(*args, **kwargs)

        if conf is None:
            conf = {}
        if application_args is None:
            application_args = []
        if archives is None:
            archives = []
        if files is None:
            files = []
        if py_files is None:
            py_files = []
        if jars is None:
            jars = []
        self.ambari_conn_id = ambari_conn_id
        self.files = files
        self.pyFiles = py_files
        self.jars = jars
        self.name = name
        self.queue = queue
        self.archives = archives
        self.numExecutors = num_executors
        self.driverCores = driver_cores
        self.executorCores = executor_cores
        self.driverMemory = driver_memory
        self.application_file = application_file
        self.main_class = main_class
        self.master = master
        self.executorMemory = executor_memory
        self.conf = conf
        self.do_xcom_push = do_xcom_push
        self.args = application_args

    def execute(self, context):
        ambari_hook = HdpAmbariHook(ambari_conn_id=self.ambari_conn_id)

        datas = {
            "className": self.main_class,
            "file": self.application_file
        }

        for attr_name in ["executorCores", "executorMemory", "driverCores",
                          "driverMemory", "numExecutors", "queue", "name"]:
            attr_value = getattr(self, attr_name)
            if attr_value is not None and attr_value != "":
                datas[attr_name] = attr_value

        for attr_name in ["archives", "jars", "pyFiles", "files", "args",
                          "conf"]:
            attr_value = getattr(self, attr_name)
            if len(attr_value) != 0 and attr_value != []:
                datas[attr_name] = attr_value

        if not ("files" in datas or "pyFiles" in datas or "className" in datas):
            raise AirflowConfigException("Request body must include files (and className) or pyFiles params")

        job_id = ambari_hook.submit_spark_job(datas)
        if self.do_xcom_push:
            context['ti'].xcom_push(key='spark_job_id', value=job_id)
