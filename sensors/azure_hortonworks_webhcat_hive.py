from airflow.utils.decorators import apply_defaults

from sensors.azure_hortonworks_base import AzureHortonWorksBase


class AzureWebHCatHive(AzureHortonWorksBase):

    @apply_defaults
    def __init__(self, job_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_id = job_id

    def poke(self, context):
        self.check_hive_status(self.job_id)
