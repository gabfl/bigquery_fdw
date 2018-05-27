import unittest
import datetime

from google.cloud import bigquery

from .. import bqclient_test
from ..bqclient import BqClient


class Test(unittest.TestCase):

    def setUp(self):
        self.query = 'SELECT count(*) FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE year=2017 AND number>1000;'
        self.key = '/opt/key/key.json'

#    def test_run_test(self):
 #       self.assertTrue(bqclient_test.run_test(self.query, self.key))

    def test_set_bq_instance(self):
        self.assertIsInstance(
            bqclient_test.set_bq_instance(), BqClient)

    def test_set_client(self):
        bq = bqclient_test.set_bq_instance()
        self.assertIsNone(bqclient_test.set_client(bq, self.key))

    def test_get_client(self):
        bq = bqclient_test.set_bq_instance()
        bqclient_test.set_client(bq, self.key)
        self.assertIsInstance(bqclient_test.get_client(
            bq), bigquery.client.Client)

    def test_run_query(self):
        bq = bqclient_test.set_bq_instance()
        bqclient_test.set_client(bq, self.key)
        self.assertIsNone(bqclient_test.run_query(bq, self.query))

        # To flush results
        bqclient_test.read_results(bq)

    def test_get_query_job(self):
        bq = bqclient_test.set_bq_instance()
        bqclient_test.set_client(bq, self.key)
        bqclient_test.run_query(bq, self.query)
        self.assertIsInstance(
            bqclient_test.get_query_job(bq), bigquery.job.QueryJob)

        # To flush results
        bqclient_test.read_results(bq)
