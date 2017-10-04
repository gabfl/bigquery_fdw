# Performance and understanding the FDW mechanism

## bigquery_fdw awareness

To understand the performance or lack of performance of a FDW, it is important to understand its awareness of your query.

Let's assume you have the following dataset in BigQuery:

```sql
 id |    date    | country_code 
----+------------+--------------
  1 | 2017-10-01 | US
  2 | 2017-10-01 | US
  3 | 2017-10-01 | US
  4 | 2017-10-01 | FR
  5 | 2017-10-01 | DE
  6 | 2017-10-02 | US
  7 | 2017-10-02 | US
(7 rows)
```

and you want to write the following query in PostgreSQL:

```sql
SELECT country_code, count(*)
FROM users
WHERE date = '2017-10-01'
GROUP BY country_code;
```

bigquery_fdw will receive the following information:

 - Columns = `country_code` and `date`
 - Qualifiers = `date = '2017-10-01'`

It will not be aware that your intent is to group results with `GROUP BY`.

## Data sent to PostgreSQL

Since bigquery_fdw is just aware of the columns and qualifiers, it will return the following to PostgreSQL:

```sql
 country_code 
--------------
 US
 US
 US
 FR
 DE
(5 rows)
```

## Processing by PostgreSQL engine

PostgreSQL will process the result receive, add the `count(*)` and `GROUP BY` logic and return the expected result:

```sql
 country_code | count 
--------------+-------
 US           |     3
 FR           |     1
 DE           |     1
```

## Performance impact

When working with BigQuery, tables tend to contain millions of billions or rows.

If a `count(*)` has a result of 1 billion rows, BigQuery will return 1 billion rows to bigquery_fdw and PostgreSQL engine will group them and return the expected result.

The lack of awareness from the original query can have a great impact on the FDW performance. You can improve this behavior by implementing the [Remote grouping and counting](remote_grouping.md) feature. 

