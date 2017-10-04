# Convert BigQuery data types to PostgreSQL data types

| BigQuery standard data type | BigQuery legacy data type | PostgreSQL equivalent | Supported in bigquery_fdw? |
|---|---|---|---|
| STRING | STRING | text | ✓ |
| BYTES | BYTES | bytea? | ✗ |
| INT64 | INTEGER | bigint | ✓ |
| FLOAT64 | FLOAT | double precision | ✓ |
| BOOL | BOOLEAN | bool | ✓ |
| *(deprecated)* | RECORD | ? | ✗ |
| TIMESTAMP | TIMESTAMP | timestamp | ✓ |
| DATE | DATE | date | ✓ |
| TIME | TIME | time | ✓ |
| DATETIME | DATETIME | timestamp | ✓ |

## External links

 - [List of PostgreSQL data types](https://www.postgresql.org/docs/9.3/static/datatype.html#DATATYPE-TABLE)
 - [List of BigQuery standard data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
 - [List of BigQuery legacy data types](https://cloud.google.com/bigquery/data-types)