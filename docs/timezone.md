# Time zone conversion support

bigquery_fdw can convert BigQuery data types `DATE` and `TIMESTAMP` to a desired time zone.

The desired time zone can be selected when creating the foreign table with the option `fdw_convert_tz`.

## Foreign table creation syntax

```sql
CREATE FOREIGN TABLE table_name (
    [...]
) SERVER bigquery_srv
OPTIONS (
    [...]
    fdw_convert_tz 'US/Eastern' -- <-- Setting the desired time zone
);
```

## Usage example

### Without time zone option

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    uuid bigint,
    timestamp timestamp
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table'
);

test=# SELECT uuid, timestamp FROM tmp LIMIT 10;
 uuid |      timestamp      
------+---------------------
 1029 | 2017-09-15 10:55:20 -- <-- Timestamps are UTC
 1030 | 2017-09-15 10:55:20
 1031 | 2017-09-15 10:55:20
 1032 | 2017-09-15 16:15:54
 1033 | 2017-09-12 13:58:20
 1034 | 2017-09-12 13:58:20
 1035 | 2017-09-12 18:19:20
 1036 | 2017-09-13 19:20:11
 1037 | 2017-09-13 19:20:11
 1038 | 2017-09-12 18:19:20
(10 rows)
```

### With time zone option

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    uuid bigint,
    timestamp timestamp
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table',
    fdw_convert_tz 'US/Eastern' -- <-- Time zone will be converted to US/Eastern
);

test=# SELECT uuid, timestamp FROM tmp LIMIT 10;
 uuid |      timestamp      
------+---------------------
 1029 | 2017-09-15 06:55:20 -- <-- Timestamps are US/Eastern
 1030 | 2017-09-15 06:55:20
 1031 | 2017-09-15 06:55:20
 1032 | 2017-09-15 12:15:54
 1033 | 2017-09-12 09:58:20
 1034 | 2017-09-12 09:58:20
 1035 | 2017-09-12 14:19:20
 1036 | 2017-09-13 15:20:11
 1037 | 2017-09-13 15:20:11
 1038 | 2017-09-12 14:19:20
(10 rows)
```

