import logging
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from HDinsightHook import HDinsightHook


def is_not_null_and_is_not_empty_str(value):
    return value is not None and value != ""


def is_not_null_and_is_not_empty_list(value):
    return len(value) != 0 and value != []


class WebHCatHiveSubmitOperator(BaseOperator):
    """
    Start a Hive query Job on a Azure HDInsight cluster.
    """

    acceptable_response_codes = [200, 201]
    statement_non_terminated_status_list = ['RUNNING', 'PREP']

    @apply_defaults
    def __init__(
            self,
            execute=None,
            sql_file=None,
            statusdir=None,
            arg=None,
            enablelog=None,
            files=None,
            callback=None,
            http_conn_id='azure_http_conn',
            cluster_name=None,
            *args,
            **kwargs):

        super(WebHCatHiveSubmitOperator, self).__init__(*args, **kwargs)

        self.query = execute
        self.file = sql_file
        self.statusdir = statusdir
        self.arg = arg
        self.enablelog = enablelog
        self.files = files
        self.callback = callback

        self.http_conn_id = http_conn_id
        self.cluster_name = cluster_name

        self.http = HDinsightHook(http_conn_id=self.http_conn_id)

    def execute(self, context):
        logging.info("Executing WebHCatHiveSubmitOperator ")
        datas = {}

        if is_not_null_and_is_not_empty_str(self.file):
            datas["file"] = self.file
        elif not is_not_null_and_is_not_empty_str(self.file) and is_not_null_and_is_not_empty_str(self.query):
            datas["execute"] = self.query
        if is_not_null_and_is_not_empty_str(self.statusdir):
            datas["statusdir"] = self.statusdir
        if is_not_null_and_is_not_empty_str(self.arg):
            datas["arg"] = self.arg
        if is_not_null_and_is_not_empty_str(self.files):
            datas["files"] = self.files
        if self.enablelog:
            datas["enablelog"] = self.enablelog
        if is_not_null_and_is_not_empty_str(self.callback):
            datas["callback"] = self.callback

        job_id = self.http.submit_hive_job(datas)
        statements_state = self.http.get_hive_job_statements(job_id=job_id)
        logging.info("Finished submitting hive script job_id: " + job_id + ")")

        while statements_state in self.statement_non_terminated_status_list:

            # todo: test execution_timeout
            time.sleep(3)
            statements_state = self.http.get_hive_job_statements(job_id=job_id)

            if statements_state == 'KILLED' or statements_state == 'FAILED':
                result = "Statement failed. (state: " + statements_state + ' )'
                logging.error(result)
                raise Exception(result)

            logging.info("Checking Hive job:  " + statements_state + ")")

        logging.info("Statement %s ", statements_state)
        logging.info("Finished executing WebHCatHiveSubmitOperator")


class LivySparkSubmitOperator(BaseOperator):
    """
    Start a Spark Job on a Azure HDInsight cluster.
    """

    acceptable_response_codes = [200, 201]
    statement_non_terminated_status_list = ['starting', 'running', "waiting", "available"]

    @apply_defaults
    def __init__(self,
                 application_file=None,
                 main_class=None,
                 master=None,
                 name=None,
                 jars=[],
                 pyFiles=[],
                 files=[],
                 executorMemory=None,
                 executorCores=None,
                 numExecutors=None,
                 driverMemory=None,
                 driverCores=None,
                 archives=[],
                 queue=None,
                 application_args=[],
                 conf={},

                 http_conn_id=None,
                 cluster_name=None,
                 *args, **kwargs):

        super(LivySparkSubmitOperator, self).__init__(*args, **kwargs)

        self.files = files
        self.pyFiles = pyFiles
        self.jars = jars
        self.name = name
        self.queue = queue
        self.archives = archives
        self.numExecutors = numExecutors
        self.driverCores = driverCores
        self.executorCores = executorCores
        self.driverMemory = driverMemory
        self.application_file = application_file
        self.main_class = main_class
        self.master = master
        self.executorMemory = executorMemory
        self.conf = conf
        self.application_args = application_args
        self.http_conn_id = http_conn_id
        self.cluster_name = cluster_name

        self.http = HDinsightHook(http_conn_id=self.http_conn_id)

    def execute(self, context):
        logging.info("Executing LivySparkSubmitOperator ")

        datas = {
            "className": self.main_class,
            "file": self.application_file
        }

        if is_not_null_and_is_not_empty_str(self.executorCores):
            datas["executorCores"] = self.executorCores
        if is_not_null_and_is_not_empty_str(self.executorMemory):
            datas["executorMemory"] = self.executorMemory
        if is_not_null_and_is_not_empty_str(self.driverCores):
            datas["driverCores"] = self.driverCores
        if is_not_null_and_is_not_empty_str(self.driverMemory):
            datas["driverMemory"] = self.driverMemory
        if is_not_null_and_is_not_empty_str(self.numExecutors):
            datas["numExecutors"] = self.numExecutors
        if is_not_null_and_is_not_empty_list(self.archives):
            datas["archives"] = self.archives
        if is_not_null_and_is_not_empty_list(self.jars):
            datas["jars"] = self.jars
        if is_not_null_and_is_not_empty_list(self.pyFiles):
            datas["pyFiles"] = self.pyFiles
        if is_not_null_and_is_not_empty_list(self.files):
            datas["pyFiles"] = self.files
        if is_not_null_and_is_not_empty_list(self.application_args):
            datas["args"] = self.application_args,
        if is_not_null_and_is_not_empty_str(self.queue):
            datas["queue"] = self.queue
        if is_not_null_and_is_not_empty_str(self.name):
            datas["name"] = self.name
        if is_not_null_and_is_not_empty_list(self.conf):  # TODO Check Emty Map of key=val
            datas["conf"] = self.conf

        job_id = self.http.submit_spark_job(datas)
        statements_state = self.http.get_spark_job_statements(job_id=job_id)
        logging.info("Finished submitting spark  job_id: " + str(job_id) + ")")

        while statements_state in self.statement_non_terminated_status_list:

            # todo: test execution_timeout
            time.sleep(3)
            statements_state = self.http.get_spark_job_statements(job_id=job_id)

            if statements_state == 'error' or statements_state == 'cancelling' or statements_state == 'cancelled':
                result = "job failed. (state: " + statements_state + ' )'
                logging.error(result)
                raise Exception(result)

            logging.info("Checking spark job:  " + statements_state + ")")

        logging.info("Statement %s ", statements_state)
        logging.info("Finished executing LivySparkSubmitOperator")
