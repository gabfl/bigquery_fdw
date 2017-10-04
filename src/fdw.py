from collections import OrderedDict
from collections import namedtuple

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, INFO, DEBUG

from .bqclient import BqClient


class ConstantForeignDataWrapper(ForeignDataWrapper):

    # Default vars
    clients = {}  # Dictionnary of clients
    bq = None  # BqClient instance
    partitionPseudoColumn = 'partition_date'  # Name of the partition pseudo column
    partitionPseudoColumnValue = None  # If a partition is used, its value will be stored in this variable to return it to PostgreSQL

    def __init__(self, options, columns):
        """
            Initialize instancem, set class leval vars
        """

        super(ConstantForeignDataWrapper, self).__init__(options, columns)

        # Set options
        self.setOptions(options)

        # Set table columns
        self.columns = columns

        # Set data types mapping
        self.setDatatypes()

    def setOptions(self, options):
        """
            Set table options at class level
        """

        # Set options at class scope
        try:
            self.key = options['fdw_key']
            self.dataset = options['fdw_dataset']
            self.table = options['fdw_table']
            self.verbose = options.get('fdw_verbose')

            # Set SQL dialect
            self.standard_sql = self.setSqlDialect(options.get('fdw_sql_dialect'))
        except KeyError:
            log_to_postgres("You must specify these options when creating the FDW: fdw_key, fdw_dataset, fdw_table", ERROR)

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

    def setSqlDialect(self, standard_sql):
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

    def getClient(self):
        """
            Manage a pool of instances of BqClient
            If the user uses different private keys in different tables, this method will return
               the correct instance of BqClient class associated to the table private key
        """

        # Returns an existing instance
        if self.clients.get(self.key):
            # Verbose log
            if self.verbose:
                log_to_postgres("Use BqClient instance ID " + str(id(self.clients[self.key])), INFO)

            return self.clients[self.key]

        # Or create a new instance
        return self.setClient()

    def setClient(self):
        """
            Attempt to connect to BigQuery client
        """

        try:
            # Attempt connection
            bq = BqClient()
            bq.setClient(self.key)

            # Verbose log
            if self.verbose:
                log_to_postgres("Connection to BigQuery client with BqClient instance ID " + str(id(bq)), INFO)

            # Add to pool
            self.clients[self.key] = bq

            return bq
        except RuntimeError:
            log_to_postgres("Connectiont to BigQuery client with key `" + self.key + "` failed", ERROR)

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

        # Returns instance of BqClient for `self.key`
        self.bq = self.getClient()

        # Prepare query
        query, parameters = self.buildQuery(quals, columns)

        # Run query
        self.bq.runAsyncQuery(query, parameters, self.dialect)

        # Return query output
        for row in self.bq.readResult():
            # Create an ordered dict with the column name and value
            # Example: `OrderedDict([('column1', 'value1'), ('column2', value2)])`
            line = OrderedDict()
            for i, column in enumerate(columns):
                if column != self.partitionPseudoColumn:  # Except for the partition pseudo column
                    line[column] = row[i]
                else:  # Fallback for partition pseudo column
                    line[column] = self.partitionPseudoColumnValue

            # # Verbose log
            # if self.verbose:
            #     log_to_postgres(line, INFO)

            yield line

        # Reset partition pseudo column value
        self.partitionPseudoColumnValue = None

    def buildQuery(self, quals, columns):
        """
            Builds a BigQuery query
        """

        # Set query var
        query = ''

        # Add SELECT clause
        query += self.buildSelectClause(columns)

        # Add FROM clause
        query += " FROM " + self.dataset + "." + self.table + " "

        # Add WHERE clause
        clause, parameters = self.buildWhereClause(quals)
        query += clause

        # Verbose log
        if self.verbose:
            log_to_postgres("Prepared query: `" + query + "`", INFO)

        return query, parameters

    def buildSelectClause(self, columns):
        """
            Build the SELECT clause of the SQL query
        """

        clause = ''

        # Add SELECT clause
        clause += "SELECT "
        for column in columns:
            if column != self.partitionPseudoColumn:  # Except for the partition pseudo column
                clause += column + ", "
            else:
                clause += "null as " + column + ", "  # Partition pseudo column is forced to return `null`

        # Remove final `, `
        clause = clause.strip(', ')

        return clause

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
                    clause += "_PARTITIONTIME = ?"
                    parameters.append(self.setParameter(qual.field_name, 'TIMESTAMP', qual.value))  # Force data type to `TIMESTAMP`

                    # Store the value to return it to PostgreSQL
                    self.partitionPseudoColumnValue = self.bq.varToString(qual.value)
                else:
                    clause += str(qual.field_name) + " " + str(self.getOperator(qual.operator)) + " ?"
                    parameters.append(self.setParameter(qual.field_name, self.getBigQueryDatatype(qual.field_name), qual.value))

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
        # Exhaustive lisT: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#operators
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
            log_to_postgres("Operator `" + operator + "` is not currently supported", ERROR)

    def getBigQueryDatatype(self, column):
        """
            Returns the BigQuery standard SQL data type of a PostgreSQL column

            Example: `column1` has the PostgreSQL type `bigint` which is called `INT64` in BigQuery standard SQL
        """

        # Get PostgreSQL column type
        # Example: `timestamp without time zone`
        pgDatatype = self.columns[column].base_type_name

        for datatype in self.datatypes:  # For each known data types
            if datatype.postgres == pgDatatype:  # If the PostgreSQL data type matches the known data type
                return datatype.bq_standard  # Returns equivalent BigQuery data type

        # Return a default data type in an attempt to save the day
        return 'STRING'

    def setParameter(self, column, type, value):
        """
            Set a parameter in BigQuery client
        """

        # Verbose log
        if self.verbose:
            log_to_postgres("Add query parameter `" + self.bq.varToString(value) + "` for column `" + column + "` witht he type `" + type + "`", INFO)

        return self.bq.setParameter(type, value)
