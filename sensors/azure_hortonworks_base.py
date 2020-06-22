from airflow import AirflowException
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from hooks.hortonworks_ambari_hook import HdpAmbariHook


class AzureHortonWorksBase(BaseSensorOperator):

    @apply_defaults
    def __init__(self, ambari_conn_id='hortonworks_ambari_default',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ambari_conn_id = ambari_conn_id
        self.hook: HdpAmbariHook = HdpAmbariHook(ambari_conn_id=self.ambari_conn_id)

        self.non_terminated_status_list = ['INIT', 'starting', 'running', 'waiting',
                                           'available', 'not_started', 'busy', 'RUNNING', 'PREP']

        self.failed_status_list = ['error', 'cancelling', 'cancelled', 'shutting_down',
                                   'dead', 'killed', 'KILLED', 'FAILED']

    def check_spark_status(self, job_id) -> bool:
        statements_state = self.hook.get_spark_job_status(job_id) or 'INIT'
        if statements_state in self.non_terminated_status_list:
            self.log.debug("Checking Hive job: %s", statements_state)
            return False
        elif statements_state in self.failed_status_list:
            result = "job failed. state: %s", statements_state
            self.log.error(result)
            raise AirflowException(result)
        else:
            self.log.debug("Checking Spark job: %s", statements_state)
            return True

    def check_hive_status(self, job_id) -> bool:
        status = self.hook.get_hive_job_status(job_id)
        statements_state = status["state"]
        statements_exit_value = status["exitValue"]
        # Check submitted job's result
        if statements_state in self.failed_status_list or statements_exit_value != "0":
            result = "job failed. state: %s, exitValue: ", statements_state, statements_exit_value
            self.log.error(result)
            raise AirflowException(result)
        elif statements_state in self.non_terminated_status_list:
            self.log.debug("Checking Hive job: %s", statements_state)
            return False
        else:
            self.log.debug("Statement %s ", statements_state)
            self.log.debug(
                f"Finished executing WebHCatHiveSubmitOperator with status: {statements_state} : {statements_exit_value}")
            return True
