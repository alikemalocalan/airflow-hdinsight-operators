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
from urllib.parse import urlencode

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.hortonworks_ambari_hook import HdpAmbariHook


class AzureWebHCatHiveSubmitOperator(BaseOperator):
    """
    Submit and Start a Hive query Job on a Azure HDInsight cluster.

    See https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Hive

    One of either "execute" or "file" is required.

    :param ambari_conn_id: connection id of Azure HDInsight cluster
            which will be used to Hortonworks rest service
    :type ambari_conn_id: str
    :param execute: String containing an entire, short Hive program to run.
    :type execute: str
    :param file:    HDFS file name of a Hive program to run.
    :type file: str
    :param arg: Set a program arguments.
            example : hive_execution_engine=tez;hive_db_name=default
    :type arg: str
    :param files:   Comma-separated files to be copied to the map reduce cluster.
            This parameter was introduced in Hive 0.12.0.
    :type files: str
    :param enablelog:   If statusdir is set and enablelog is "true",
            collect Hadoop job configuration and logs into
            a directory named $statusdir/logs after the job finishes.
            Both completed and failed attempts are logged.
            The layout of subdirectories in $statusdir/logs is:
            logs/$job_id (directory for $job_id)
            logs/$job_id/job.xml.html
            logs/$job_id/$attempt_id (directory for $attempt_id)
            logs/$job_id/$attempt_id/stderr
            logs/$job_id/$attempt_id/stdout
            logs/$job_id/$attempt_id/syslog

            This parameter was introduced in Hive 0.12.0.
    :type enablelog: str
    :param statusdir:    A directory where WebHCat will write the status of the Hive job.
            If provided, it is the caller's responsibility to remove this directory when done.
    :type statusdir: str
    :param callback:    Define a URL to be called upon job completion.
            You may embed a specific job ID into this URL using $jobId.
            This tag will be replaced in the callback URL with this job's job ID.
    :type callback: str
    """

    @apply_defaults
    def __init__(self,
                 ambari_conn_id='hortonworks_ambari_default',
                 execute=None,
                 file=None,
                 statusdir=None,
                 arg=None,
                 enablelog=None,
                 files=None,
                 callback=None,
                 timeout=5,
                 *args,
                 **kwargs
                 ):

        super(AzureWebHCatHiveSubmitOperator, self).__init__(*args, **kwargs)

        self.ambari_conn_id = ambari_conn_id
        self.query = execute
        self.file = file
        self.statusdir = statusdir
        self.arg = arg
        self.enablelog = enablelog
        self.files = files
        self.callback = callback
        self.timeout = timeout

    def execute(self, context):
        ambari_hook = HdpAmbariHook(ambari_conn_id=self.ambari_conn_id)
        datas = {}

        hive_defines = None
        datas["user.name"] = ambari_hook.cluster_name

        for attr_name in ["statusdir", "files", "callback"]:
            attr_value = getattr(self, attr_name)
            if attr_value is not None and attr_value != "":
                datas[attr_name] = attr_value

        if is_not_null_and_is_not_empty_str(self.file):
            datas["file"] = self.file
        elif not is_not_null_and_is_not_empty_str(self.file) and is_not_null_and_is_not_empty_str(self.query):
            datas["execute"] = self.query

        if is_not_null_and_is_not_empty_str(self.arg):
            # define arg params for hive =>  key1=value1;key2=value2
            hive_defines = urlencode([("define", x) for x in str(self.arg).split(";")])

        if self.enablelog:
            datas["enablelog"] = self.enablelog

        if is_not_null_and_is_not_empty_str(hive_defines):
            self.query = urlencode(datas) + "&" + hive_defines
        else:
            self.query = urlencode(datas)

        ambari_hook.submit_hive_job(self.query, self.timeout)


def is_not_null_and_is_not_empty_str(value):
    return value is not None and value != ""
