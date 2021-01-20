import unittest
from unittest.mock import patch
from collections import OrderedDict
import datetime

import multicorn
from google.cloud import bigquery

from ..bqclient import BqClient
from ..fdw import ConstantForeignDataWrapper


class Test(unittest.TestCase):

    def setUp(self):
        # Set options
        self.options = {
            'fdw_dataset': 'bigquery-public-data.usa_names',
            'fdw_table': 'usa_1910_current',
            'fdw_verbose': 'true',
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

    def test_setOptions_2(self):
        # Should create a `KeyError` exception which should call log_to_postgres()
        o = self.options
        del o['fdw_dataset']
        self.assertIsNone(self.fdw.setOptions(o))

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

    def test_setOptionSqlDialect_4(self):
        self.fdw.verbose = False
        self.fdw.setOptionSqlDialect()
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

    def test_setOptionCasting_2(self):
        # Nothing should happen if no casting options have been set
        casting = ''
        self.assertIsNone(self.fdw.setOptionCasting(casting))

    def test_getClient(self):
        self.fdw.setClient()
        self.assertIsInstance(self.fdw.getClient(), BqClient)

    def test_getClient_2(self):
        self.fdw.verbose = False
        self.fdw.setClient()
        self.assertIsInstance(self.fdw.getClient(), BqClient)

    def test_setClient(self):
        self.assertIsInstance(self.fdw.setClient(), BqClient)

    def test_setClient_2(self):
        self.fdw.verbose = False
        self.assertIsInstance(self.fdw.setClient(), BqClient)

    @patch.object(bigquery.Client, 'from_service_account_json')
    def test_setClient_3(self, patched):
        # Should return `None` and call log_to_postgres() if the BigQuery client cannot be set correctly
        patched.return_value = None
        self.assertIsNone(self.fdw.setClient())

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

    def test_buildQuery_2(self):
        self.fdw.verbose = False
        self.fdw.bq = self.fdw.getClient()
        query, parameters = self.fdw.buildQuery(self.quals, self.columns)

        self.assertIsInstance(query, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_buildQuery_3(self):
        # Test with grouping option
        self.fdw.groupBy = True

        self.fdw.bq = self.fdw.getClient()
        query, parameters = self.fdw.buildQuery(self.quals, self.columns)

        self.assertIsInstance(query, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_buildQuery_4(self):
        # Test with grouping option but no columns sent to buildQuery()
        self.fdw.groupBy = True

        self.fdw.bq = self.fdw.getClient()
        query, parameters = self.fdw.buildQuery(self.quals, None)

        self.assertIsInstance(query, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_buildColumnList(self):
        self.assertEqual(self.fdw.buildColumnList(
            self.columns), 'state  as state, gender  as gender, year  as year, name  as name, number  as number')

    def test_buildColumnList_2(self):
        self.assertEqual(self.fdw.buildColumnList(
            self.columns, 'GROUP_BY'), 'state , gender , year , name , number')

    def test_buildColumnList_3(self):
        # Test with counting pseudo column
        c = self.columns
        c['_fdw_count'] = multicorn.ColumnDefinition(
            column_name='_fdw_count', type_oid=20, base_type_name='bigint')

        self.assertEqual(self.fdw.buildColumnList(
            c), 'state  as state, gender  as gender, year  as year, name  as name, number  as number, count(*)  as _fdw_count')

    def test_buildColumnList_4(self):
        # Test with counting pseudo column
        c = self.columns
        c['_fdw_count'] = multicorn.ColumnDefinition(
            column_name='_fdw_count', type_oid=20, base_type_name='bigint')

        self.assertEqual(self.fdw.buildColumnList(
            c, 'GROUP_BY'), 'state , gender , year , name , number')

    def test_buildColumnList_5(self):
        # Test with partition pseudo column
        c = self.columns
        c['partition_date'] = multicorn.ColumnDefinition(
            column_name='partition_date', type_oid=0, base_type_name='date')

        self.assertEqual(self.fdw.buildColumnList(
            c), 'state  as state, gender  as gender, year  as year, name  as name, number  as number, _PARTITIONTIME  as partition_date')

    def test_buildColumnList_6(self):
        # Test with partition pseudo column
        c = self.columns
        c['partition_date'] = multicorn.ColumnDefinition(
            column_name='partition_date', type_oid=0, base_type_name='date')

        self.assertEqual(self.fdw.buildColumnList(
            c, 'GROUP_BY'), 'state , gender , year , name , number , _PARTITIONTIME')

    def test_buildColumnList_7(self):
        # Test with a datetime
        c = self.columns
        c['datetime'] = multicorn.ColumnDefinition(
            column_name='datetime', type_oid=0, base_type_name='timestamp without time zone')

        self.assertEqual(self.fdw.buildColumnList(
            c, 'GROUP_BY'), 'state , gender , year , name , number , datetime')

    def test_buildColumnList_8(self):
        # Test `SELECT *`
        self.assertEqual(self.fdw.buildColumnList(None), '*')

    def test_buildColumnList_9(self):
        # Test no columns when grouping by
        self.assertEqual(self.fdw.buildColumnList(None, 'GROUP_BY'), '')

    def test_setTimeZone(self):
        self.fdw.convertToTz = 'US/Eastern'
        self.assertEqual(self.fdw.setTimeZone(
            'column1', 'DATE').strip(), 'DATE(column1, "US/Eastern")')

    def test_setTimeZone_2(self):
        self.fdw.convertToTz = 'US/Eastern'
        self.assertEqual(self.fdw.setTimeZone(
            'column1', 'DATETIME').strip(), 'DATETIME(column1, "US/Eastern")')

    def test_setTimeZone_3(self):
        self.fdw.convertToTz = None
        self.assertEqual(self.fdw.setTimeZone(
            'column1', 'DATE').strip(), 'column1')

    def test_setTimeZone_4(self):
        self.fdw.convertToTz = None
        self.assertEqual(self.fdw.setTimeZone(
            'column1', 'DATETIME').strip(), 'column1')

    def test_castColumn(self):
        # Options are a dict casted as a string
        casting = '{"number": "STRING"}'
        self.fdw.setOptionCasting(casting)

        self.assertEqual(self.fdw.castColumn(
            'number', 'number', 'INT64'), 'CAST(number as STRING)')

    def test_castColumn_2(self):
        # Options are a dict casted as a string
        casting = '{"number": "STRING"}'
        self.fdw.setOptionCasting(casting)

        # Casting should fail on columns not in the casting rules
        self.assertEqual(self.fdw.castColumn(
            'year', 'year', 'INT64'), 'year')

    def test_castColumn_3(self):
        # Options are a dict casted as a string
        casting = '{"number": "SOME_INVALID_TYPE"}'
        self.fdw.setOptionCasting(casting)

        # Casting should fail on invalid types
        self.assertEqual(self.fdw.castColumn(
            'number', 'number', 'INT64'), 'number')

    def test_castColumn_4(self):
        # Options are a dict casted as a string
        casting = '{"number": "STRING"}'
        self.fdw.setOptionCasting(casting)

        # Casting should fail on invalid types
        self.assertEqual(self.fdw.castColumn(
            'number', 'number', 'SOME_INVALID_TYPE'), 'number')

    def test_addColumnAlias(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column'), ' as some_column')

    def test_addColumnAlias_2(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column', False), '')

    def test_buildWhereClause(self):
        self.fdw.bq = self.fdw.getClient()
        clause, parameters = self.fdw.buildWhereClause(self.quals)

        self.assertIsInstance(clause, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_buildWhereClause_2(self):
        # Test with partition pseudo column
        q = self.quals
        q.append(multicorn.Qual(field_name='partition_date',
                                operator='=',
                                value=datetime.datetime(2018, 5, 27, 19, 53, 42).date()))

        self.fdw.bq = self.fdw.getClient()
        clause, parameters = self.fdw.buildWhereClause(q)

        self.assertIsInstance(clause, str)
        self.assertIsInstance(parameters, list)
        for parameter in parameters:
            self.assertIsInstance(
                parameter, bigquery.query.ScalarQueryParameter)

    def test_buildWhereClause_3(self):
        # Test with no quals
        self.fdw.bq = self.fdw.getClient()
        clause, parameters = self.fdw.buildWhereClause(None)

        self.assertIsInstance(clause, str)
        self.assertEqual(clause, '')
        self.assertIsInstance(parameters, list)
        self.assertEqual(parameters, [])

    def test_getOperator(self):
        self.assertEqual(self.fdw.getOperator('='), '=')

    def test_getOperator_2(self):
        self.assertEqual(self.fdw.getOperator('~~'), 'LIKE')

    def test_getOperator_3(self):
        self.assertEqual(self.fdw.getOperator('!~~'), 'NOT LIKE')

    def test_getOperator_4(self):
        # Test an invalid operator, should return `None` and call log_to_postgres()
        self.assertIsNone(self.fdw.getOperator('**'))

    def test_getBigQueryDatatype(self):
        self.assertEqual(self.fdw.getBigQueryDatatype('number'), 'INT64')

    def test_getBigQueryDatatype_2(self):
        self.assertEqual(self.fdw.getBigQueryDatatype(
            'number', 'legacy'), 'INTEGER')

    def test_getBigQueryDatatype_3(self):
        # Test with a column that has an invalid type
        c = self.columns
        c['some_column'] = multicorn.ColumnDefinition(
            column_name='some_column', type_oid=0, base_type_name='invalid_type')
        self.fdw.columns = c

        # Should default to `STRING`
        self.assertEqual(self.fdw.getBigQueryDatatype('some_column'), 'STRING')

    def test_setParameter(self):
        self.fdw.bq = self.fdw.getClient()
        self.assertIsInstance(self.fdw.setParameter(
            'column', 'STRING', 'some string'), bigquery.query.ScalarQueryParameter)
