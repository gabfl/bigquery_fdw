from collections import OrderedDict, namedtuple

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, INFO, DEBUG

from .bqclient import BqClient


class ConstantForeignDataWrapper(ForeignDataWrapper):

    # Default vars
    client = None  # BqClient instance
    partitionPseudoColumn = 'partition_date'  # Name of the partition pseudo column
    # Pseudo column to fetch `count(*)` when using the remote counting and grouping feature
    countPseudoColumn = '_fdw_count'
    castingRules = None  # Dict of casting rules when using the `fdw_casting` option

    def __init__(self, options, columns):
        """
            Initialize instance, set class level vars
        """

        super(ConstantForeignDataWrapper, self).__init__(options, columns)

        # Set options
        self.setOptions(options)

        # Set table columns
        self.columns = columns

        # Set data types and conversion rules mapping
        self.setDatatypes()
        self.setConversionRules()

    def setOptions(self, options):
        """
            Set table options at class level
        """

        # Set options at class scope
        try:
            self.dataset = options['fdw_dataset']
            self.table = options['fdw_table']
            self.convertToTz = options.get('fdw_convert_tz')

            # Set verbose option
            self.setOptionVerbose(options.get('fdw_verbose'))

            # Set SQL dialect
            self.setOptionSqlDialect(options.get('fdw_sql_dialect'))

            # Set grouping option
            self.setOptionGroupBy(options.get('fdw_group'))

            # Set casting rules
            self.setOptionCasting(options.get('fdw_casting'))
        except KeyError:
            log_to_postgres(
                "You must specify these options when creating the FDW: fdw_dataset, fdw_table", ERROR)

    def setDatatypes(self):
        """
            Set data types mapping
        """

        # Create a named tuple
        datatype = namedtuple('datatype', 'postgres bq_standard bq_legacy')

        self.datatypes = [
            datatype('text', 'STRING', 'STRING'),
            # datatype('bytea', 'BYTES', 'BYTES'), # Not supported, need testing for support
            datatype('bigint', 'INT64', 'INTEGER'),
            datatype('double precision', 'FLOAT64', 'FLOAT'),
            datatype('boolean', 'BOOL', 'BOOLEAN'),
            datatype('timestamp without time zone', 'TIMESTAMP', 'TIMESTAMP'),
            datatype('date', 'DATE', 'DATE'),
            datatype('time without time zone', 'TIME', 'TIME'),
            datatype('timestamp without time zone', 'DATETIME', 'DATETIME'),
        ]

    def setConversionRules(self):
        """
            Set list of allowed conversion rules
        """

        # Create a named tuple
        conversionRule = namedtuple(
            'conversionRule', 'bq_standard_from bq_standard_to')

        self.conversionRules = [
            conversionRule('INT64', ['BOOL', 'FLOAT64', 'INT64', 'STRING']),
            conversionRule('FLOAT64', ['FLOAT64', 'INT64', 'STRING']),
            conversionRule('BOOL', ['BOOL', 'INT64', 'STRING']),
            conversionRule('STRING', ['BOOL', 'BYTES', 'DATE', 'DATETIME',
                                      'FLOAT64', 'INT64', 'STRING', 'TIME', 'TIMESTAMP']),
            conversionRule('BYTES', ['BYTES', 'STRING']),
            conversionRule(
                'DATE', ['DATE', 'DATETIME', 'STRING', 'TIMESTAMP']),
            conversionRule(
                'DATETIME', ['DATE', 'DATETIME', 'STRING', 'TIME', 'TIMESTAMP']),
            conversionRule('TIME', ['STRING', 'TIME']),
            conversionRule(
                'TIMESTAMP', ['DATE', 'DATETIME', 'STRING', 'TIME', 'TIMESTAMP']),
            conversionRule('ARRAY', ['ARRAY']),
            conversionRule('STRUCT', ['STRUCT']),
        ]

    def setOptionSqlDialect(self, standard_sql=None):
        """
            Set a flag for the SQL dialect.
            It can be `standard` or `legacy`. `standard` will be the default
        """

        self.dialect = 'standard'

        if standard_sql == 'legacy':
            self.dialect = 'legacy'

        # Verbose log
        if self.verbose:
            log_to_postgres("Set SQL dialect to `" + self.dialect + "`", INFO)

    def setOptionGroupBy(self, group):
        """
            Set a flag `self.groupBy` as `True` if `group` contains the string 'true'
            Otherwise, set it as `False`
        """

        if group == 'true':
            self.groupBy = True
            return

        self.groupBy = False

    def setOptionVerbose(self, verbose):
        """
            Set a flag `self.verbose` as `True` if `verbose` contains the string 'true'
            Otherwise, set it as `False`
        """

        if verbose == 'true':
            self.verbose = True
            return

        self.verbose = False

    def setOptionCasting(self, castingRules):
        """
            Conversion rules are received as a string, for example: '{"key": "FLOAT64", "datetime": "DATE"}'

            The string will be converted to a dict
        """

        if castingRules:
            # Cast string as a dict
            try:
                import ast
                self.castingRules = ast.literal_eval(castingRules)
            except Exception as e:
                log_to_postgres(
                    "fdw_casting conversion failed: `" + str(e) + "`", ERROR)

            # For security reasons, ensure that the string was correctly casted as a dict
            try:
                if type(self.castingRules) is not dict:
                    raise ValueError('fdw_casting format is incorrect.')
            except Exception as e:
                log_to_postgres(
                    "fdw_casting conversion failed: `" + str(e) + "`", ERROR)

    def getClient(self):
        """
            Manage a pool of instances of BqClient
            If the user uses different private keys in different tables, this method will return
               the correct instance of BqClient class associated to the table private key
        """

        # Returns an existing instance
        if self.client:
            return self.client

        # Or create a new instance
        return self.setClient()

    def setClient(self):
        """
            Attempt to connect to BigQuery client
        """

        try:
            # Attempt connection
            bq = BqClient()
            bq.setClient()

            # Verbose log
            if self.verbose:
                log_to_postgres(
                    "Connection to BigQuery client with BqClient instance ID " + str(id(bq)), INFO)

            # Add to pool
            self.client = bq

            return bq
        except RuntimeError:
            log_to_postgres(
                "Connection to BigQuery client failed", ERROR)

    def execute(self, quals, columns):
        """
            Executes a query
        """

        # # Verbose log
        # if self.verbose:
        #     log_to_postgres('Quals...', INFO)
        #     log_to_postgres(quals, INFO)
        #     log_to_postgres('Columns...', INFO)
        #     log_to_postgres(columns, INFO)

        # Returns instance of BqClient
        client = self.getClient()

        # Prepare query
        query, parameters = self.buildQuery(quals, columns)

        # Run query
        client.runQuery(query, parameters, self.dialect)

        # Return query output
        for row in client.readResult():
            # Create an ordered dict with the column name and value
            # Example: `OrderedDict([('column1', 'value1'), ('column2', value2)])`
            line = OrderedDict()
            for column in columns:
                line[column] = row[column]

            yield line

    def buildQuery(self, quals, columns):
        """
            Builds a BigQuery query
        """

        # Set query var
        query = ''

        # Add SELECT clause
        query += 'SELECT ' + self.buildColumnList(columns)

        # Add FROM clause
        query += " FROM `" + self.dataset + "." + self.table + "` "

        # Add WHERE clause
        clause, parameters = self.buildWhereClause(quals)
        query += clause

        # Add group by
        if self.groupBy:
            groupByColumns = self.buildColumnList(columns, 'GROUP_BY')
            if groupByColumns:
                query += ' GROUP BY ' + \
                    self.buildColumnList(columns, 'GROUP_BY')

        # Verbose log
        if self.verbose:
            log_to_postgres("Prepared query: `" + query + "`", INFO)

        return query, parameters

    def buildColumnList(self, columns, usage='SELECT'):
        """
            Build the SELECT clause of the SQL query
        """

        clause = ''

        # Disable aliases for Group By
        useAliases = True
        if usage == 'GROUP_BY':
            useAliases = False

        if columns:  # If we have columns
            for column in columns:
                if column == self.countPseudoColumn:  # Pseudo column to count grouped rows
                    if usage == 'SELECT':
                        clause += "count(*) " + \
                            self.addColumnAlias(column, useAliases) + ", "
                elif column == self.partitionPseudoColumn:  # Partition pseudo column
                    clause += "_PARTITIONTIME " + \
                        self.addColumnAlias(column, useAliases) + ", "
                else:  # Any other column
                    # Get column data type
                    dataType = self.getBigQueryDatatype(column)

                    # Save column original name
                    columnOriginalName = column

                    # If the data type is a date or a timestamp
                    if dataType in ['DATE', 'TIMESTAMP']:
                        column = self.setTimeZone(column, dataType)

                    # Data type casting
                    column = self.castColumn(
                        column, columnOriginalName, dataType)

                    clause += column + " " + \
                        self.addColumnAlias(
                            columnOriginalName, useAliases) + ", "

            # Remove final `, `
            clause = clause.strip(', ')
        elif usage == 'SELECT':  # Otherwise fetch all
            clause += "*"

        return clause

    def setTimeZone(self, column, dataType):
        """
            If the option `fdw_convert_tz` is used, convert the time zone automatically from UTC to the desired time zone
        """

        # Option is set
        if self.convertToTz:
            if dataType == 'DATE':  # BigQuery column type is `DATE`
                return 'DATE(' + column + ', "' + self.convertToTz + '") '
            else:  # BigQuery column type is `TIMESTAMP`
                return 'DATETIME(' + column + ', "' + self.convertToTz + '") '

        # Option is not set
        return column

    def castColumn(self, column, columnOriginalName, dataType):
        """
            If the option `fdw_casting` is used, this method will attempt to cast the column to the new type
        """

        if self.castingRules and columnOriginalName in self.castingRules:  # If we have casting rule for this column
            # Get desired casting
            castTo = self.castingRules[columnOriginalName]

            # Find if we have a matching rule
            rule = [
                conversionRule for conversionRule in self.conversionRules if conversionRule.bq_standard_from == dataType.upper()]

            if rule:
                # Check if casting from the original data type to the new one is supported
                if castTo.upper() in rule[0].bq_standard_to:
                    return 'CAST(' + column + ' as ' + castTo.upper() + ')'
                else:
                    log_to_postgres("Casting from the data type `" + dataType.upper(
                    ) + "` to the data type `" + castTo.upper() + "` is not permitted.", ERROR)
            else:
                log_to_postgres(
                    "Casting from the data type `" + dataType.upper() + "` is not permitted.", ERROR)

        # Option is not set
        return column

    def addColumnAlias(self, alias, useAliases=True):
        """
            Returns a string "as `alias`" if `useAliases` is `True`
        """

        if useAliases:
            return " as " + alias

        return ''

    def buildWhereClause(self, quals):
        """
            Build the WHERE clause of the SQL query
        """

        clause = ''
        parameters = []

        # Add WHERE clause
        # `quals` example: `[Qual('test', '=', 'test 2'), Qual('test', '~~', '3')]`
        if quals:
            clause += "WHERE "
            for qual in quals:
                if qual.field_name == self.partitionPseudoColumn:
                    clause += "_PARTITIONTIME " + \
                        str(self.getOperator(qual.operator)) + " ?"

                    # Format date as a timestamp
                    value = qual.value.strftime("%Y-%m-%d 00:00:00")

                    # Force data type to `TIMESTAMP`
                    parameters.append(self.setParameter(
                        qual.field_name, 'TIMESTAMP', value))
                else:
                    clause += str(qual.field_name) + " " + \
                        str(self.getOperator(qual.operator)) + " ?"
                    parameters.append(self.setParameter(
                        qual.field_name, self.getBigQueryDatatype(qual.field_name), qual.value))

                # Add ` AND `
                clause += " AND "

        # Remove final ` AND `
        clause = clause.strip(' AND ')

        return (clause, parameters)

    def getOperator(self, operator):
        """
            Validate operator
        """

        # List of BigQuery operators supported
        # Exhaustive list: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#operators
        # Non listed operators may or may not work
        operators = ['=', '<', '>', '<=', '>=', '!=', '<>', 'LIKE', 'NOT LIKE']

        # Mapping between multicorn operators and BigQuery operators
        mapping = {}
        mapping['~~'] = 'LIKE'
        mapping['!~~'] = 'NOT LIKE'

        if operator in operators:  # Operator is natively supported
            return operator
        elif operator in mapping:  # Multicorn operator has a BigQuery equivalent
            return mapping[operator]
        else:  # Operator is not supported
            log_to_postgres(
                "Operator `" + operator + "` is not currently supported", ERROR)

    def getBigQueryDatatype(self, column, dialect='standard'):
        """
            Returns the BigQuery standard SQL data type of a PostgreSQL column

            Example: `column1` has the PostgreSQL type `bigint` which is called `INT64` in BigQuery standard SQL
        """

        # Get PostgreSQL column type
        # Example: `timestamp without time zone`
        pgDatatype = self.columns[column].base_type_name

        for datatype in self.datatypes:  # For each known data types
            if datatype.postgres == pgDatatype:  # If the PostgreSQL data type matches the known data type
                # Returns equivalent BigQuery data type
                if dialect == 'legacy':
                    return datatype.bq_legacy
                else:
                    return datatype.bq_standard

        # Return a default data type in an attempt to save the day
        return 'STRING'

    def setParameter(self, column, type_, value):
        """
            Set a parameter in BigQuery client
        """

        # Verbose log
        if self.verbose:
            log_to_postgres(
                "Add query parameter `" + self.client.varToString(value) + "` for column `" + column + "` with the type `" + type_ + "`", INFO)

        return self.client.setParameter(type_, value)
