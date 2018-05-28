import unittest
from collections import OrderedDict

import multicorn
from google.cloud import bigquery

from ..bqclient import BqClient
from ..fdw import ConstantForeignDataWrapper


class Test(unittest.TestCase):

    def setUp(self):
        # Set options
        self.options = {
            'fdw_key': '/opt/key/key.json',
            'fdw_dataset': 'bigquery-public-data.usa_names',
            'fdw_table': 'usa_1910_current',
            'fdw_verbose': False,
            'fdw_sql_dialect': 'standard',
            'fdw_group': 'false',
            'fdw_casting': 'false',
        }

        # Set column list (ordered dict of ColumnDefinition from Multicorn)
        self.columns = OrderedDict([
            ('state', multicorn.ColumnDefinition(
                column_name='state', type_oid=25, base_type_name='text')),
            ('gender', multicorn.ColumnDefinition(
                column_name='gender', type_oid=25, base_type_name='text')),
            ('year', multicorn.ColumnDefinition(
                column_name='year', type_oid=20, base_type_name='bigint')),
            ('name', multicorn.ColumnDefinition(
                column_name='name', type_oid=25, base_type_name='text')),
            ('number', multicorn.ColumnDefinition(
                column_name='number', type_oid=20, base_type_name='bigint'))
        ])

        # Define Quals as defined by Multicorn
        self.quals = [
            multicorn.Qual(field_name='number', operator='>', value=1000),
            multicorn.Qual(field_name='year', operator='=', value=2017),
        ]

        # Set instance of ConstantForeignDataWrapper
        self.fdw = ConstantForeignDataWrapper(self.options, self.columns)

    def test_setOptions(self):
        self.assertIsNone(self.fdw.setOptions(self.options))

    def test_setDatatypes(self):
        self.fdw.setDatatypes()
        self.assertIsInstance(self.fdw.datatypes, list)
        for datatype in self.fdw.datatypes:
            self.assertIsInstance(datatype, tuple)
            self.assertIsInstance(datatype.postgres, str)
            self.assertIsInstance(datatype.bq_standard, str)
            self.assertIsInstance(datatype.bq_legacy, str)

    def test_setConversionRules(self):
        self.fdw.setConversionRules()
        self.assertIsInstance(self.fdw.conversionRules, list)
        for conversionRule in self.fdw.conversionRules:
            self.assertIsInstance(conversionRule, tuple)
            self.assertIsInstance(conversionRule.bq_standard_from, str)
            self.assertIsInstance(conversionRule.bq_standard_to, list)

    def test_setOptionSqlDialect(self):
        self.fdw.setOptionSqlDialect()
        self.assertEqual(self.fdw.dialect, 'standard')

    def test_setOptionSqlDialect_2(self):
        self.fdw.setOptionSqlDialect('legacy')
        self.assertEqual(self.fdw.dialect, 'legacy')

    def test_setOptionSqlDialect_3(self):
        self.fdw.setOptionSqlDialect('non_existent')
        # Should fallback to `standard`
        self.assertEqual(self.fdw.dialect, 'standard')

    def test_setOptionGroupBy(self):
        self.fdw.setOptionGroupBy('true')
        self.assertTrue(self.fdw.groupBy)

    def test_setOptionGroupBy_2(self):
        self.fdw.setOptionGroupBy('false')
        self.assertFalse(self.fdw.groupBy)

    def test_setOptionVerbose(self):
        self.fdw.setOptionVerbose('true')
        self.assertTrue(self.fdw.verbose)

    def test_setOptionVerbose_2(self):
        self.fdw.setOptionVerbose('false')
        self.assertFalse(self.fdw.verbose)

    def test_setOptionCasting(self):
        # Options are a dict casted as a string
        casting = '{"column1": "STRING", "column2": "DATE", "column3": "TIMESTAMP"}'
        self.fdw.setOptionCasting(casting)
        self.assertIsInstance(self.fdw.castingRules, dict)
        for column, cast in self.fdw.castingRules.items():
            self.assertTrue(column in ['column1', 'column2', 'column3'])
            self.assertTrue(cast in ['STRING', 'DATE', 'TIMESTAMP'])

    def test_getClient(self):
        self.fdw.setClient()
        self.assertIsInstance(self.fdw.getClient(), BqClient)

    def test_setClient(self):
        self.assertIsInstance(self.fdw.setClient(), BqClient)

    def test_execute(self):
        self.fdw.setClient()
        execute = self.fdw.execute(self.quals, self.columns.keys())

        for row in execute:
            # Ensure that the row is an OrderedDict
            self.assertIsInstance(row, OrderedDict)
            # Compare the keys of each row with the expected columns
            self.assertEqual(set(row.keys()), set(
                {'state', 'gender', 'year', 'name', 'number'}))

    def test_buildQuery(self):
        self.fdw.bq = self.fdw.getClient()
        query, parameters = self.fdw.buildQuery(self.quals, self.columns)
        self.assertIsInstance(query, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_addColumnAlias(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column'), ' as some_column')

    def test_addColumnAlias_2(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column', False), '')
