# bigquery_fdw: BigQuery Foreign Data Wrapper for PostgreSQL

[![Pypi](https://img.shields.io/pypi/v/bigquery-fdw.svg)](https://pypi.org/project/bigquery-fdw/)
[![Build Status](https://travis-ci.org/gabfl/bigquery_fdw.svg?branch=main)](https://travis-ci.org/gabfl/bigquery_fdw)
[![codecov](https://codecov.io/gh/gabfl/bigquery_fdw/branch/main/graph/badge.svg)](https://codecov.io/gh/gabfl/bigquery_fdw)
[![MIT licensed](https://img.shields.io/badge/license-MIT-green.svg)](https://raw.githubusercontent.com/gabfl/bigquery_fdw/main/LICENSE)

bigquery_fdw is a BigQuery foreign data wrapper for PostgreSQL using [Multicorn](https://github.com/Segfault-Inc/Multicorn).

It allows to write queries in PostgreSQL SQL syntax using a foreign table. It supports most of BigQuery's [data types](docs/data_types.md) and [operators](docs/operators.md).

## Features and limitations

 - Table partitioning is supported. [You can use partitions in your SQL queries](docs/table_partitioning.md).
 - Queries are parameterized when sent to BigQuery
 - BigQuery's standard SQL support (legacy SQL is not supported)
 - Authentication works with a "[Service Account](docs/service_account.md)" Json private key

[Read more](docs/README.md).

## Requirements

 - PostgreSQL >= 9.5
 - Python >= 3.4

## ⚠️ Migrating to version 1.8 from versions 1.7 and below

Starting with version 1.8, the `fdw_key` option is deprecated and replaced with a default environment variable. See [Authentication](#Authentication).

## Get started

### Using docker

See [getting started with Docker](docs/docker.md)

### Installation on Debian/Ubuntu

#### Dependencies required to install bigquery_fdw:

You need to install the following dependencies:

```bash
# Install required packages
apt-get update
apt-get install --yes postgresql-server-dev-12 python3-setuptools python3-dev make gcc git
```

For PostgresSQL 9.X, install `postgresql-server-dev-9.X` instead of `postgresql-server-dev-12`.

All PostgreSQL versions from 9.2 to 12 should be supported. Building Multicorn against PostgreSQL 13 is currently not working properly (as of 1/21/2013).

#### Installation

```bash
# Install Multicorn
# gabfl/Multicorn is a fork of Segfault-Inc/Multicorn that adds better support for Python3.
# You may chose to build against the original project instead.
git clone git://github.com/gabfl/Multicorn.git && cd Multicorn
make && make install

# Install bigquery_fdw
pip3 install bigquery-fdw
```

Major dependencies installed automatically during the installation process:

 - [Google Cloud BigQuery](https://pypi.org/project/google-cloud-bigquery/)
 - [Multicorn](https://github.com/Segfault-Inc/Multicorn)

## Authentication

bigquery_fdw relies on [Google Cloud API's default authentication](https://cloud.google.com/docs/authentication/getting-started#linux-or-macos). 

Your need to have an environment variable `GOOGLE_APPLICATION_CREDENTIALS` that has to be accessible by bigquery_fdw. Setting environment variables varies depending on OS but for Ubuntu or Debian, the preferred way is to edit `/etc/postgresql/[version]/main/environment` and add:
```
GOOGLE_APPLICATION_CREDENTIALS = '/path/to/key.json'
```

Restarting PostgreSQL is required for the environment variable to be loaded.

## Usage

We recommend testing the [BigQuery client connectivity](docs/test_client.md) before trying to use the FDW.

With `psql`:

```sql
CREATE EXTENSION multicorn;

CREATE SERVER bigquery_srv FOREIGN DATA WRAPPER multicorn
OPTIONS (
    wrapper 'bigquery_fdw.fdw.ConstantForeignDataWrapper'
);

CREATE FOREIGN TABLE my_bigquery_table (
    column1 text,
    column2 bigint
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset  'my_dataset',
    fdw_table 'my_table'
);
```

## Options

List of options implemented in `CREATE FOREIGN TABLE` syntax:

| Option | Default | Description |
|-----|----|----|
| `fdw_dataset` | - | BigQuery dataset name |
| `fdw_table` | - | BigQuery table name |
| `fdw_convert_tz` | - | Convert BigQuery time zone for dates and timestamps to selected time zone. Example: `'US/Eastern'`. |
| `fdw_group` |  `'false'` | See [Remote grouping and counting](docs/remote_grouping.md). |
| `fdw_casting` |  - | See [Casting](docs/casting.md). |
| `fdw_verbose` | `'false'` | Set to `'true'` to output debug information in PostrgeSQL's logs |
| `fdw_sql_dialect` | `'standard'` | BigQuery SQL dialect. Currently only `standard` is supported. |

## More documentation

See [bigquery_fdw documentation](docs/README.md).
