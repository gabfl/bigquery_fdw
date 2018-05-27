import unittest

from ..fdw import ConstantForeignDataWrapper


class Test(unittest.TestCase):

    def setUp(self):
        options = {
            'fdw_key': '/opt/key/key.json',
            'fdw_dataset': 'bigquery-public-data.usa_names',
            'fdw_table': 'usa_1910_current',
            'fdw_verbose': False,
        }
        columns = {}
        self.fdw = ConstantForeignDataWrapper(options, columns)

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

    def test_addColumnAlias(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column'), ' as some_column')

    def test_addColumnAlias_2(self):
        self.assertEqual(self.fdw.addColumnAlias(
            'some_column', False), '')
