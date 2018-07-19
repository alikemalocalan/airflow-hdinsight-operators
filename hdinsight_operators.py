import logging
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from HDinsightHook import HDinsightHook

"""Reference:
https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Hive
"""


class WebHCatHiveSubmitOperator(BaseOperator):
    """
    Start a Hive query Job on a Cloud DataProc cluster.
    """

    acceptable_response_codes = [200, 201]
    statement_non_terminated_status_list = ['RUNNING', 'PREP']

    @apply_defaults
    def __init__(
            self,
            execute=None,
            sql_file=None,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            hive_properties=None,
            hive_jars=None,
            http_conn_id='livy_http_conn',
            *args,
            **kwargs):

        super(WebHCatHiveSubmitOperator, self).__init__(*args, **kwargs)

        self.http_conn_id = http_conn_id
        self.query = execute
        self.file = sql_file
        self.variables = variables
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.properties = hive_properties
        self.jars = hive_jars

        self.http = HDinsightHook(http_conn_id=self.http_conn_id)

    def execute(self, context):
        logging.info("Executing LivyHiveSubmitOperator ")

        job_id = self.http.submit_hive_job(self.file)
        statements_state = self.http.get_job_statements(job_id=job_id)
        logging.info("Finished submitting hive script. statement_id: " + job_id + ")")

        while statements_state in self.statement_non_terminated_status_list:

            # todo: test execution_timeout
            time.sleep(3)
            statements_state = self.http.get_job_statements(job_id=job_id)

            if statements_state == 'KILLED' or statements_state == 'FAILED':
                result = "Statement failed. (state: " + statements_state + ' )'
                logging.error(result)
                raise Exception(result)

            logging.info("Checking Hive job:  " + statements_state + ")")

        logging.info("Statement %s ", statements_state)
        logging.info("Finished executing LivyHiveSubmitOperator")
