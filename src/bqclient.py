"""
    See:
    https://cloud.google.com/bigquery/docs/reference/libraries
    https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery/usage.html?highlight=insert_data
    https://github.com/GoogleCloudPlatform/python-docs-samples/tree/master/bigquery/cloud-client
"""

import datetime

from google.cloud import bigquery, bigquery_storage
from google import auth


class BqClient:
    # Set vars
    client = None
    queryJob = None
    location = None  # Override dataset location

    def setClient(self):
        """
        Set BigQuery client with a Json key
        """

        try:
            self.client = bigquery.Client()
        except auth.exceptions.DefaultCredentialsError as e:
            raise RuntimeError(
                "BigQuery client is not instantiated properly: " + str(e)
            )

    def getClient(self):
        """
        Returns `client`
        """

        return self.client

    def runQuery(self, query, parameters=[], sqlDialect="standard"):
        """
        Run BigQuery query
        """

        if self.client:
            if parameters:  # Parameterized query
                # Prepare job configuration
                job_config = bigquery.QueryJobConfig()
                job_config.query_parameters = parameters

                self.queryJob = self.client.query(
                    query, job_config=job_config, location=None
                )
            else:  # Non parameterized query
                self.queryJob = self.client.query(query, location=None)

            # Set SQL dialect
            self.queryJob.UseLegacySQL = False
            # if sqlDialect == 'legacy':
            #     self.queryJob.UseLegacySQL = True
            # else:
            #     self.queryJob.UseLegacySQL = False

        else:
            raise RuntimeError(
                "BigQuery client is not instantiated properly (from `runQuery`)."
            )

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
            result = self.queryJob.result()
            return result.to_dataframe(create_bqstorage_client=True).to_dict(
                orient="records"
            )
        else:
            raise RuntimeError("No query is pending a result.")

    def setParameter(self, type_, value):
        """
        Prepare a parameter for a parameterized query
        As documented by Google, only standard SQL syntax supports parameters in queries

        See https://github.com/googleapis/python-bigquery/blob/main/samples/client_query_w_positional_params.py
        """

        return bigquery.ScalarQueryParameter(
            # Set the name to None to use positional parameters (? symbol
            # in the query).  Note that you cannot mix named and positional
            # parameters.
            None,
            type_,
            self.createBQParamValue(value),
        )

    def createBQParamValue(self, var):
        """
        Generate appropriate parameter value for BigQuery query parameter

        Examples:
        `createBQParamValue(1234)` -> 1234
        `createBQParamValue('1234')` -> '1234'
        `createBQParamValue(datetime.datetime.now())` -> '2017-10-03 11:47:54'
        `createBQParamValue(datetime.datetime.now().date())` -> '2017-10-03'
        """

        if type(var) is datetime.date:
            param_value = var.strftime("%Y-%m-%d")
        elif type(var) is datetime.datetime:
            param_value = var.strftime("%Y-%m-%d %H:%M:%S")
        elif type(var) in (int, float):
            param_value = var
        else:
            param_value = str(var)

        return param_value
