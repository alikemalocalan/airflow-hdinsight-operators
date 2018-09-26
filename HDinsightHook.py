import json
import logging
import time
from builtins import str

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HDinsightHook():

    def __init__(self, cluster_name, cluster_username, cluster_password):

        self.acceptable_response_codes = [200, 201]

        self.headers = {
            'Content-Type': "application/x-www-form-urlencoded"
        }
        self.hive_endpoint = "templeton/v1/"
        self.spark_endpoint = "livy/batches"

        self.cluster_name = cluster_name
        self.username = cluster_username
        self.password = cluster_password
        self.params = {"user.name": self.username}
        self.port = 80
        self.base_url = "https://{0}.azurehdinsight.net/".format(self.cluster_name)

    def submit_hive_job(self, datas):
        """
        Reference:
        https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Hive
        """

        method = "POST"
        statement_non_terminated_status_list = ['RUNNING', 'PREP']

        submit_endpoint = self.hive_endpoint + "hive"

        logging.info("Submiting hive  Script: " + str(datas))

        response = self.http_rest_call(method=method,
                                       endpoint=submit_endpoint,
                                       data=datas)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            result = response_json["id"]
        else:
            result = "Statement   returned \'{0}\'." \
                .format(str(response.status_code))
            logging.error(result + "\n" + str(response.json()))
            raise Exception(result)

        statements_state = self.get_hive_job_statements(job_id=result)
        logging.info("Finished submitting hive script job_id: " + result + ")")

        while statements_state in statement_non_terminated_status_list:

            # todo: test execution_timeout
            time.sleep(5)
            statements_state = self.get_hive_job_statements(job_id=result)

            if statements_state == 'KILLED' or statements_state == 'FAILED':
                result = "Statement failed. (state: " + statements_state + ' )'
                logging.error(result)
                raise Exception(result)

            logging.info("Checking Hive job:  " + statements_state + ")")

        logging.info("Statement %s ", statements_state)
        logging.info("Finished executing WebHCatHiveSubmitOperator")

    def submit_spark_job(self, datas):
        statement_non_terminated_status_list = ['starting', 'running', "waiting", "available"]
        """
        Reference:
        http://livy.incubator.apache.org/docs/latest/rest-api.html
        """
        method = "POST"
        submit_endpoint = self.spark_endpoint
        datas["user.name"] = self.username

        logging.info("Submiting spark Job: %s ", json.dumps(datas))
        response = self.http_rest_call(method=method,
                                       endpoint=submit_endpoint,
                                       data=json.dumps(datas))

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            result = response_json["id"]
        else:
            result = "Statement didn\'t return {0}. Returned \'{1}\'." \
                .format(str(self.acceptable_response_codes),
                        str(response.status_code))
            logging.error(result)
            raise Exception(result)

        statements_state = self.http.get_spark_job_statements(job_id=result)
        logging.info("Finished submitting spark  job_id: " + str(result) + ")")

        while statements_state in statement_non_terminated_status_list:

            # todo: test execution_timeout
            time.sleep(3)
            statements_state = self.http.get_spark_job_statements(job_id=result)

            if statements_state == 'error' or statements_state == 'cancelling' or statements_state == 'cancelled':
                result = "job failed. (state: " + statements_state + ' )'
                logging.error(result)
                raise Exception(result)

            logging.info("Checking spark job:  " + statements_state + ")")

        logging.info("Statement %s ", statements_state)
        logging.info("Finished executing LivySparkSubmitOperator")

    def http_rest_call(self, method, endpoint, data=None):
        url = self.base_url + endpoint

        if method == 'GET':
            response = requests.get(
                url,
                params=self.params,
                headers=self.headers,
                auth=(self.username, self.password))
        else:
            response = requests.post(
                url,
                data=data,
                auth=(self.username, self.password),
                headers=self.headers
            )

        logging.debug("Sending to %s  %s", response.request)

        logging.debug("status_code: " + str(response.status_code))
        logging.debug("response_as_json: " + response.text)
        return response

    def get_hive_job_statements(self, job_id):
        method = "GET"
        status_endpoint = self.hive_endpoint + "jobs/" + str(job_id)
        response = self.http_rest_call(method=method, endpoint=status_endpoint)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            statements = response_json["status"]["state"]
            return statements
        else:
            result = "Call to get the session statement response  returned \'{0}\'." \
                .format(str(response.status_code))
            logging.error(result + "\n" + response.text)
            raise Exception(result)

    def get_spark_job_statements(self, job_id):
        method = "GET"
        status_endpoint = self.spark_endpoint + "/" + str(job_id)
        response = self.http_rest_call(method=method, endpoint=status_endpoint)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            statements = response_json["state"]
            return statements
        else:
            result = "Call to get the session statement response didn\'t return {0}. Returned \'{1}\'." \
                .format(str(self.acceptable_response_codes),
                        str(response.status_code))
            logging.error(result)
            raise Exception(result)
