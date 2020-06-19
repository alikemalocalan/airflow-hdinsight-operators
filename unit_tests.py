import unittest

import mock
import requests
from airflow.models import DagBag, Connection

from hooks.hortonworks_ambari_hook import HdpAmbariHook


class TestAzureExampleDAG(unittest.TestCase):
    """Check HelloWorldDAG expectation"""

    def setUp(self):
        self.dagbag = DagBag()
        self.hdp_Ambari_Hook = HdpAmbariHook()

    @mock.patch('hooks.hortonworks_ambari_hook.HdpAmbariHook.get_connection')
    def test_hdp_ambari_Hook_connection(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http',
                          host='localhost', schema='https')
        mock_get_connection.return_value = conn
        hook = HdpAmbariHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'https://localhost')

    @mock.patch('hooks.hortonworks_ambari_hook.HdpAmbariHook.get_connection')
    def test_submit_spark_job(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http',
                          host='localhost', schema='https')
        mock_get_connection.return_value = conn
        hook = HdpAmbariHook()
        hook.get_conn({})

        datas = {
            "className": "com.test",
            "file": "wasp://test"
        }
        # test that it raises

        # ti = TaskInstance(task=start_cluster_task, execution_date=datetime.now())
        # start_cluster_task.execute(ti.get_template_context())
        with self.assertRaises(requests.exceptions.ConnectionError) as ex:
            self.hdp_Ambari_Hook.submit_spark_job(datas)
