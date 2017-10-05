# Casting (convert column type to another type)

bigquery_fdw has an option to implement BigQuery's casting feature:

> Cast syntax is used in a query to indicate that the result type of an expression should be converted to some other type.

## Foreign table creation syntax

```sql
CREATE FOREIGN TABLE table_name (
    [...]
) SERVER bigquery_srv
OPTIONS (
    [...]
    fdw_casting '{"column1": "CAST_TO_TYPE", "column2": "CAST_TO_TYPE", ...}'
);
```

## Usage example

### Without casting option

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    column1 bigint,
    timestamp timestamp
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table',
    fdw_key '/opt/bigquery_fdw/user.json'
);

test=# SELECT timestamp FROM tmp LIMIT 5;
      timestamp      
---------------------
 2017-09-14 09:12:32
 2017-09-14 09:12:32
 2017-09-14 09:12:32
 2017-09-12 00:36:16
 2017-09-12 17:23:12
(5 rows)
```

### With casting option

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    column1 bigint,
    timestamp timestamp
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table',
    fdw_key '/opt/bigquery_fdw/user.json',
    fdw_casting '{"timestamp": "DATE"}'
);

test=# SELECT timestamp FROM tmp LIMIT 5;
      timestamp      
---------------------
 2017-09-11 00:00:00
 2017-09-11 00:00:00
 2017-09-11 00:00:00
 2017-09-11 00:00:00
 2017-09-11 00:00:00
(5 rows)
```


## External links

 - [BigQuery casting](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#casting)
