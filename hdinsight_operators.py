import logging
from urllib.parse import urlencode

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode, DeploymentProperties

from HDinsightHook import HDinsightHook


class WebHCatHiveSubmitOperator(BaseOperator):
    """
    Start a Hive query Job on a Azure HDInsight cluster.
    """

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
            cluster_name=None,
            cluster_username=None,
            cluster_password=None,
            *args,
            **kwargs):

        super(WebHCatHiveSubmitOperator, self).__init__(*args, **kwargs)

        self.cluster_password = cluster_password
        self.cluster_username = cluster_username
        self.cluster_name = cluster_name
        self.query = execute
        self.file = sql_file
        self.statusdir = statusdir
        self.arg = arg
        self.enablelog = enablelog
        self.files = files
        self.callback = callback

        self.http = HDinsightHook(self.cluster_name, self.cluster_username, self.cluster_password)

    def execute(self, context):
        logging.info("Executing WebHCatHiveSubmitOperator ")
        datas = {}

        hive_defines = None
        datas["user.name"] = self.cluster_username
        if is_not_null_and_is_not_empty_str(self.file):
            datas["file"] = self.file
        elif not is_not_null_and_is_not_empty_str(self.file) and is_not_null_and_is_not_empty_str(self.query):
            datas["execute"] = self.query
        if is_not_null_and_is_not_empty_str(self.statusdir):
            datas["statusdir"] = self.statusdir
        if is_not_null_and_is_not_empty_str(self.arg):
            # define arg params for hive =>  key1=value1;key2=value2
            hive_defines = urlencode([("define", x) for x in str(self.arg).split(";")])
        if is_not_null_and_is_not_empty_str(self.files):
            datas["files"] = self.files
        if self.enablelog:
            datas["enablelog"] = self.enablelog
        if is_not_null_and_is_not_empty_str(self.callback):
            datas["callback"] = self.callback

        if is_not_null_and_is_not_empty_str(hive_defines):
            self.query = urlencode(datas) + "&" + hive_defines
        else:
            self.query = urlencode(datas)

        self.http.submit_hive_job(self.query)


class LivySparkSubmitOperator(BaseOperator):
    """
    Start a Spark Job on a Azure HDInsight cluster.
    """

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
                 cluster_name=None,
                 cluster_username=None,
                 cluster_password=None,
                 *args, **kwargs):

        super(LivySparkSubmitOperator, self).__init__(*args, **kwargs)

        self.cluster_password = cluster_password
        self.cluster_username = cluster_username
        self.cluster_name = cluster_name
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

        self.http = HDinsightHook(self.cluster_name, self.cluster_username, self.cluster_password)

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

        self.http.submit_spark_job(datas)


def is_not_null_and_is_not_empty_str(value):
    return value is not None and value != ""


def is_not_null_and_is_not_empty_list(value):
    return len(value) != 0 and value != []


class HDInsightCreateClusterOperator(BaseOperator):
    """Refences
https://github.com/Azure-Samples/resource-manager-python-resources-and-groups

https://docs.microsoft.com/en-us/python/api/overview/azure/key-vault?toc=%2Fpython%2Fazure%2FTOC.json&view=azure-python
"""

    def __init__(self, client_id, secret, tenant, subscription_id, resource_group_name, resource_group_location,
                 deploy_name, template_json, parameters_json, *args, **kwargs):
        super(HDInsightCreateClusterOperator, self).__init__(*args, **kwargs)
        self.secret = secret
        self.client_id = client_id
        self.tenant = tenant
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location
        self.deploy_name = deploy_name
        self.template_json = template_json
        self.parameters_json = parameters_json

    def create_cluster(self, ):
        client = ResourceManagementClient(self.get_credential(), self.subscription_id)
        """Deploy the template to a resource group."""
        client.resource_groups.get(
            self.resource_group_name,
            self.resource_group_location
        )

        deployment_properties = DeploymentProperties(
            mode=DeploymentMode.incremental,
            template=self.template_json,
            parameters=self.parameters_json)

        deployment_async_operation = client.deployments.create_or_update(
            self.resource_group_name,
            self.deploy_name,
            deployment_properties
        )
        deployment_async_operation.wait()
        return deployment_async_operation.result()

    def execute(self, context):
        logging.info("Executing HDInsightCreateClusterOperator ")
        self.create_cluster()
        logging.info("Finished executing HDInsightCreateClusterOperator")

    def get_credential(self):
        return ServicePrincipalCredentials(
            client_id=self.client_id,
            secret=self.secret,
            tenant=self.tenant,
        )


class HDInsightDeleteClusterOperator(BaseOperator):

    def __init__(self, client_id, secret, tenant, subscription_id, resource_group_name, resource_group_location,
                 deploy_name, *args, **kwargs):
        super(HDInsightDeleteClusterOperator, self).__init__(*args, **kwargs)
        self.secret = secret
        self.client_id = client_id
        self.tenant = tenant
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location
        self.cluster_name = deploy_name

    def delete_cluster(self):
        client = HDInsightManagementClient(self.get_credential(), self.subscription_id)
        delete_poller = client.clusters.delete(self.resource_group_name,
                                               cluster_name=self.cluster_name)
        delete_poller.wait()
        return delete_poller.result()

    def execute(self, context):
        logging.info("Executing HDInsightDeleteClusterOperator ")
        self.delete_cluster()
        logging.info("Finished executing HDInsightDeleteClusterOperator")

    def get_credential(self):
        return ServicePrincipalCredentials(
            client_id=self.client_id,
            secret=self.secret,
            tenant=self.tenant,
        )
