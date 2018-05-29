# Use bigquery_fdw on docker

## Pre-requisite

You will need:
 - To clone bigquery_fdw on your compoter
 - To [create a "Service account"](service_account.md) with the [correct permissions](service_account_permissions.md)

## Get started

### Clone the project and launch the docker image

```bash
git clone https://github.com/gabfl/bigquery_fdw.git

docker run \
-v ~/path/to/bigquery_fdw:/opt/bigquery_fdw \
-v ~/path/to/key.json:/opt/key/key.json \
-ti gabfl/bigquery_fdw
```

### Install bigquery_fdw

```bash
python3 setup.py install
su postgres -c 'psql -f sample_sql/setup.sql -d fdw'
```

### Run your first query

```bash
pgcli -U super fdw
```

```sql
fdw> SELECT * FROM usa_names WHERE year=2017 AND number>1000 LIMIT 5;
+---------+----------+--------+----------+----------+
| state   | gender   | year   | name     | number   |
|---------+----------+--------+----------+----------|
| CA      | F        | 2017   | Abigail  | 1536     |
| CA      | F        | 2017   | Amelia   | 1069     |
| CA      | F        | 2017   | Penelope | 1084     |
| CA      | F        | 2017   | Mia      | 2588     |
| CA      | F        | 2017   | Sophia   | 2430     |
+---------+----------+--------+----------+----------+
SELECT 5
Time: 3.992s (3 seconds)
```

## What is in the image?

The docker images comes with:
 - PostgreSQL 10 installed
 - Multicorn installed and compiled
 - All the packages required by bigquery_fdw
 - The setup.sql file comes pre-loaded with the foreign table `bigquery-public-data.usa_names.usa_1910_current` to test queries.