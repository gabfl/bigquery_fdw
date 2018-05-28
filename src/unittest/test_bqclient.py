import unittest
from unittest.mock import patch
import datetime

from google.cloud import bigquery

from ..bqclient import BqClient


class Test(unittest.TestCase):

    def setUp(self):
        self.key = '/opt/key/key.json'
        self.query = 'SELECT count(*) FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE year=2017 AND number>1000;'
        self.parameterizedQuery = {
            'query': 'SELECT count(*) FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE year=? AND number>?;',
            'parameters': [('INT64', 2017), ('INT64', 1000)]
        }

        # Set BqClient instance
        self.bc = BqClient()

        # Override location (need for public datasets used for unit tests)
        self.bc.location = 'US'

    def test_setClient(self):
        self.bc.setClient(self.key)
        self.assertIsInstance(self.bc.client, bigquery.client.Client)

    @patch.object(bigquery.Client, 'from_service_account_json')
    def test_setClient_2(self, patched):
        # Should return a RuntimeError if the BigQuery client cannot be set correctly
        patched.return_value = None
        self.assertRaises(RuntimeError, self.bc.setClient, self.key)

    def test_getClient(self):
        self.bc.setClient(self.key)
        self.assertIsInstance(self.bc.getClient(), bigquery.client.Client)

    def test_runQuery(self):
        self.bc.setClient(self.key)
        self.assertIsNone(self.bc.runQuery(self.query))

        # Dump results
        [row for row in self.bc.readResult()]

    def test_runQuery_2(self):
        # Prepare parameters to test parameterized query
        parameters = []
        for type_, value in self.parameterizedQuery['parameters']:
            parameters.append(self.bc.setParameter(type_, value))

        # Run parameterized query
        self.bc.setClient(self.key)
        self.assertIsNone(self.bc.runQuery(
            self.parameterizedQuery['query'], parameters))

        # Dump results
        [row for row in self.bc.readResult()]

    def test_runQuery_3(self):
        self.assertRaises(RuntimeError, self.bc.runQuery, self.query)

    def test_readResult(self):
        self.bc.setClient(self.key)
        self.bc.runQuery(self.query)

        for row in self.bc.readResult():
            self.assertIsInstance(row, bigquery.table.Row)

    def test_readResult_2(self):
        # Test RuntimeError when no result is pending
        self.assertRaises(RuntimeError, self.bc.readResult)

    def test_setParameter(self):
        self.assertIsInstance(self.bc.setParameter(
            'STRING', 'some string'), bigquery.query.ScalarQueryParameter)

    def test_varToString(self):
        self.assertEqual(self.bc.varToString(1234), '1234')

    def test_varToString_2(self):
        self.assertEqual(self.bc.varToString('1234'), '1234')

    def test_varToString_3(self):
        self.assertEqual(self.bc.varToString(datetime.datetime(
            2018, 5, 27, 19, 53, 42)), '2018-05-27 19:53:42')

    def test_varToString_4(self):
        self.assertEqual(self.bc.varToString(datetime.datetime(
            2018, 5, 27, 19, 53, 42).date()), '2018-05-27')
