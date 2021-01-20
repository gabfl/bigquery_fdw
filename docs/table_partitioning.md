## Table partitioning

BigQuery **table partitioning is supported**. When partitioning a table, BigQuery creates a pseudo column called `_PARTITIONTIME`.

To use partitions, you need to add a column `partition_date` with the type `date` in the foreign table definition, for example:

```sql
CREATE FOREIGN TABLE my_bigquery_table (
    column1 text,
    column2 bigint,
    partition_date date -- <-- partition!
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table'
);
```

You can then use the partition in the `WHERE` clause:

```sql
SELECT column1, column2
FROM my_bigquery_table
WHERE column1 = 'abc' AND partition_date = '2017-12-01'
```
