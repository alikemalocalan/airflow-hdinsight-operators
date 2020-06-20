import unittest
from unittest import mock

from airflow import DAG, AirflowException
from airflow.utils import dates

from sensors.azure_hortonworks_livyspark import AzureLivySpark
from sensors.azure_hortonworks_webhcat_hive import AzureWebHCatHive


class HdpSensorTest(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': dates.days_ago(1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    @mock.patch('hooks.hortonworks_ambari_hook.HdpAmbariHook', autospec=True)
    def test_poke_hive(self, mock_hook):
        sensor = AzureWebHCatHive(
            job_id="123",
            task_id='hive_sensor',
            dag=self.dag,
            check_options={'timeout': 5}
        )

        with self.assertRaises(AirflowException) as ex:
            sensor.poke(None)

    @mock.patch('hooks.hortonworks_ambari_hook.HdpAmbariHook', autospec=True)
    def test_poke_spark(self, mock_hook):
        sensor = AzureLivySpark(
            job_id="123",
            task_id='spark_sensor',
            dag=self.dag,
            check_options={'timeout': 5}
        )

        with self.assertRaises(AirflowException) as ex:
            sensor.poke(None)
