import json
import logging

import requests
import urllib3
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from builtins import str

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _run_and_check(method, session, prepped_request, extra_options):
    """
    Grabs extra options like timeout and actually runs the request,
    checking for the result
    """
    extra_options = extra_options or {}

    response = session.send(
        prepped_request,
        stream=extra_options.get("stream", False),
        verify=extra_options.get("verify", False),
        proxies=extra_options.get("proxies", {}),
        cert=extra_options.get("cert"),
        timeout=extra_options.get("timeout"),
        allow_redirects=extra_options.get("allow_redirects", True))

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Tried rewrapping, but not supported. This way, it's possible
        # to get reason and code for failure by checking first 3 chars
        # for the code, or do a split on ':'
        logging.error("HTTP error: %s", response.reason)
        if method not in ('GET', 'POST'):
            # The sensor uses GET, so this prevents filling up the log
            # with the body every time the GET 'misses'.
            # That's ok to do, because GETs should be repeatable and
            # all data should be visible in the log (no post data)
            logging.error(response.text)
        raise AirflowException(str(response.status_code) + ":" + response.content.__str__())
    return response


class HDinsightHook(BaseHook):
    # url = "https://{}.azurehdinsight.net/{}"
    acceptable_response_codes = [200, 201]

    def __init__(self, http_conn_id='http_default'):
        self.http_conn_id = http_conn_id
        self.headers = {
            'X-Requested-By': "user",
            'Content-Type': "application/json"
        }
        self.hive_endpoint = "templeton/v1/"
        self.spark_endpoint = "livy/batches"
        self.conn = self.get_connection(self.http_conn_id)
        self.username = str(self.conn.login)
        self.params = {"user.name": self.username}

    def get_conn(self):

        session = requests.Session()

        if "://" in self.conn.host:
            self.base_url = self.conn.host
        else:
            # schema defaults to HTTP
            schema = self.conn.schema if self.conn.schema else "https"
            self.base_url = schema + "://" + self.conn.host

        # if self.conn.port:
        #     self.base_url = self.base_url + ":" + str(self.conn.port) + "/"
        if self.conn.login:
            session.auth = (self.conn.login, self.conn.password)
        if self.headers:
            session.headers.update(self.headers)

        return session

    def submit_hive_job(self, datas):
        """
        Reference:
        https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference+Hive
        """

        method = "POST"

        submit_endpoint = self.hive_endpoint + "hive"

        logging.info("Submiting hive  Script: " + str(datas))

        response = self.http_rest_call(method=method,
                                       endpoint=submit_endpoint,
                                       data=datas)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            return response_json["id"]
        else:
            result = "Statement didn\'t return {0}. Returned \'{1}\'." \
                .format(str(self.acceptable_response_codes),
                        str(response.status_code))
            logging.error(result)
            raise Exception(result)

    def submit_spark_job(self, datas):
        """
        Reference:
        http://livy.incubator.apache.org/docs/latest/rest-api.html
        """
        method = "POST"
        submit_endpoint = self.spark_endpoint

        logging.info("Submiting spark Job: %s ", json.dumps(datas))
        response = self.http_rest_call(method=method,
                                       endpoint=submit_endpoint,
                                       data=json.dumps(datas))

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            return response_json["id"]
        else:
            result = "Statement didn\'t return {0}. Returned \'{1}\'." \
                .format(str(self.acceptable_response_codes),
                        str(response.status_code))
            logging.error(result)
            raise Exception(result)

    def http_rest_call(self, method, endpoint, data=None):

        logging.debug(
            "HTTP REQUEST: (method: {0}, endpoint: {1}, data: {2}, headers: {3})"
                .format(str(method),
                        str(endpoint),
                        str(data),
                        str(self.headers)))

        response = self._run(method, endpoint, data)

        logging.debug("status_code: " + str(response.status_code))
        logging.debug("response_as_json: " + str(response.json()))

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
            result = "Call to get the session statement response didn\'t return {0}. Returned \'{1}\'." \
                .format(str(self.acceptable_response_codes),
                        str(response.status_code))
            logging.error(result)
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

    def _run(self, method, endpoint, data=None, extra_options=None):
        """
        Performs the request
        """
        extra_options = extra_options or {}

        session = self.get_conn()

        url = self.base_url + endpoint

        if method == 'GET':
            # GET uses params
            req = requests.Request(method,
                                   url,
                                   params=self.params,
                                   headers=self.headers)

        else:
            req = requests.Request(method,
                                   url,
                                   params=self.params,
                                   data=data,
                                   headers=self.headers)

        prepped_request = session.prepare_request(req)
        logging.debug("Sending to %s  %s", req.data, req.params)
        return _run_and_check(method, session, prepped_request, extra_options)
