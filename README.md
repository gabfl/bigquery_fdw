# bigquery_fdw: BigQuery Foreign Data Wrapper for PostgreSQL

bigquery_fdw is a BigQuery foreign data wrapper for PostgreSQL using [Multicorn](https://github.com/Kozea/Multicorn).

It allows to write queries in PostgreSQL SQL syntax using a foreign table. It supports most of BigQuery's [data types](docs/data_types.md) and [operators](docs/operators.md).

## Features and limitations

 - Table partitioning is supported. [You can use partitions in your SQL queries](docs/table_partitioning.md).
 - Queries are parameterized when sent to BigQuery
 - BigQuery's standard SQL support (legacy SQL is not supported)
 - Authentication works with a "[Service Account](docs/service_account.md)" Json private key

[Read more](docs/README.md).

## Requirements

 - PostgreSQL >= 9.5
 - Python 3

## Dependencies

### Dependencies required to install bigquery_fdw:

 - `postgresql-server-dev-X.Y`
 - `python3-pip`
 - `python3-dev`
 - `make`
 - `gcc`

### Major dependencies installed automatically during the installation process:

 - [Google Cloud BigQuery](https://pypi.org/project/google-cloud-bigquery/)
 - [Multicorn](https://github.com/Kozea/Multicorn)

## Installation

```bash
# Install `setuptools` if necessary
pip3 install --upgrade setuptools

# Install Multicorn
git clone git://github.com/Kozea/Multicorn.git && cd Multicorn
export PYTHON_OVERRIDE=python3
make && make install

# Install bigquery_fdw
pip3 install bigquery-fdw
```

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
    fdw_table 'my_table',
    fdw_key '/opt/bigquery_fdw/user.json'
);
```

## Options

List of options implemented in `CREATE FOREIGN TABLE` syntax:

| Option | Default | Description |
|-----|----|----|
| `fdw_dataset` | - | BigQuery dataset name |
| `fdw_table` | - | BigQuery table name |
| `fdw_key` | - | Path to private Json key (See [Key storage recommendations](docs/key_storage.md)) |
| `fdw_convert_tz` | - | Convert BigQuery time zone for dates and timestamps to selected time zone. Example: `'US/Eastern'`. |
| `fdw_group` |  `'false'` | See [Remote grouping and counting](docs/remote_grouping.md). |
| `fdw_casting` |  - | See [Casting](docs/casting.md). |
| `fdw_verbose` | `'false'` | Set to `'true'` to output debug information in PostrgeSQL's logs |
| `fdw_sql_dialect` | `'standard'` | BigQuery SQL dialect. Currently only `standard` is supported. |

## More documentation

See [bigquery_fdw documentation](docs/README.md).
