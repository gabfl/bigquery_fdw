# Remote grouping and counting

## Introduction

### Now

To understand the remote counting and grouping feature, it is recommended to read [performance and understanding the FDW mechanism](performance_and_mechanism.md) first.

The remote grouping and counting feature allows to set a flag in bigquery_fdw to defer the grouping and counting of rows to BigQuery instead of PostgreSQL.

### & the future

PostgreSQL 10 implements [push down aggregates to remote servers](https://www.depesz.com/2016/10/25/waiting-for-postgresql-10-postgres_fdw-push-down-aggregates-to-remote-servers/):

> "it's possible for foreign data wrappers to arrange to push aggregates to the remote side instead of fetching all of the rows and aggregating them locally.
> 
> This figures to be a massive win for performance, so teach postgres_fdw to do it."

Once this feature is implemented in [Multicorn](https://github.com/Kozea/Multicorn) it will become possible to add it to bigquery_fdw as well.

## Foreign table creation syntax

```sql
CREATE FOREIGN TABLE table_name (
    [...]
    _fdw_count bigint -- <-- Pseudo column to count results
) SERVER bigquery_srv
OPTIONS (
    [...]
    fdw_group 'true' -- <-- Flag to automatically group columns
);
```

## Usage example

### Without remote grouping and counting

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    country_code text
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table'
);

test=# SELECT country_code, count(*)
       FROM tmp
       GROUP BY country_code
       ORDER BY count(*) DESC
       LIMIT 10;
 country_code | count 
--------------+-------
 US           | 25930
 FR           | 20006
 DE           |  8919
 IT           |  7579
 GB           |  6457
 AU           |  6037
 BR           |  5300
 CA           |  4310
 ES           |  4004
 PL           |  3567
(10 rows)

Time: 8096.112 ms
```

This produced the following query in BigQuery:

```sql
SELECT country_code
FROM my_dataset.my_table
```

### With remote grouping and counting

```sql
test=# DROP FOREIGN TABLE tmp;

test=# CREATE FOREIGN TABLE tmp (
    country_code text,
    _fdw_count bigint -- <-- Pseudo column to count results
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table',
    fdw_group 'true' -- <-- Flag to automatically group columns
);

test=# SELECT country_code, 
              _fdw_count -- <-- Replaces `count(*)`
       FROM geos_gp
       GROUP BY country_code, 
                _fdw_count -- <-- Has to be included in `GROUP BY`
       ORDER BY _fdw_count DESC -- <-- Replaces `count(*)
       LIMIT 10;
 country_code | _fdw_count 
--------------+------------
 US           |      25930
 FR           |      20006
 DE           |       8919
 IT           |       7579
 GB           |       6457
 AU           |       6037
 BR           |       5300
 CA           |       4310
 ES           |       4004
 PL           |       3567
(10 rows)

Time: 2464.072 ms
```

This produced the following query in BigQuery:

```sql
SELECT country_code, count(*) as _fdw_count
FROM my_dataset.my_table
GROUP BY country_code
```

**With remote counting and grouping, the query is 3.28 times faster.** The larger the tables, the greatest the performance improvement will be.
