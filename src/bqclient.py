"""
    See:
    https://cloud.google.com/bigquery/docs/reference/libraries
    https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery/usage.html?highlight=insert_data
    https://github.com/GoogleCloudPlatform/python-docs-samples/tree/master/bigquery/cloud-client
"""

import uuid
import datetime

from google.cloud import bigquery


class BqClient:

    # Set vars
    client = None
    queryJob = None

    def setClient(self, jsonKeyPath):
        """
            Set BigQuery client with a Json key
        """

        self.client = bigquery.Client.from_service_account_json(jsonKeyPath)

        if not self.client:
            raise RuntimeError('BigQuery client is not instantiated properly (from `setClient`).')

    def getClient(self):
        """
            Returns `client`
        """

        return self.client

    def runAsyncQuery(self, query, parameters=[], sqlDialect='standard'):
        """
            Run asynchronous BigQuery query
        """

        if self.client:
            self.queryJob = self.client.run_async_query(str(uuid.uuid4()), query,  query_parameters=parameters)

            # Set SQL dialect
            if sqlDialect == 'legacy':
                self.queryJob.use_legacy_sql = True
            else:
                self.queryJob.use_legacy_sql = False

            self.queryJob.begin()
            self.queryJob.result()  # Wait for job to complete.
        else:
            raise RuntimeError('BigQuery client is not instantiated properly (from `runAsyncQuery`).')

    def getQueryJob(self):
        """
            Returns `queryJob`
        """

        return self.queryJob

    def readResult(self):
        """
            Returns query result
        """

        if self.queryJob:
            destination_table = self.queryJob.destination
            destination_table.reload()
            return destination_table.fetch_data()
        else:
            raise RuntimeError('No query is pending a result.')

    def setParameter(self, type, value):
        """
            Prepare a parameter for a parameterized query
            As documented by Google, only standard SQL syntax supports parameters in queries

            See https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/cloud-client/query_params.py
        """

        return bigquery.ScalarQueryParameter(
            # Set the name to None to use positional parameters (? symbol
            # in the query).  Note that you cannot mix named and positional
            # parameters.
            None, type, self.varToString(value))

    def varToString(self, var):
        """
            Format a variable to a string

            Examples:
            `varToString(1234)` -> '1234'
            `varToString('1234')` -> '1234'
            `varToString(datetime.datetime.now())` -> '2017-10-03 11:47:54'
            `varToString(datetime.datetime.now().date())` -> '2017-10-03'
        """

        if type(var) is datetime.date:
            return var.strftime('%Y-%m-%d')
        elif type(var) is datetime.datetime:
            return var.strftime('%Y-%m-%d %H:%M:%S')

        return str(var)
