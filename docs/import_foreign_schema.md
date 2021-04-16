# IMPORT FOREIGN SCHEMA support for bigquery_fdw

If you want to attempt to more or less automatically import all of your Bigquery schema that your bigquery_fdw-hosting Postgres has access to, you can:

1. Ensure your bigquery_fdw access key is avalable. See: [BigQuery client](README.md)
1. From a user with proper access and priviledges (please modify to suit your needs):

```sql
BEGIN;
CREATE EXTENSION IF NOT EXISTS multicorn;
DROP SERVER IF EXISTS server_name CASCADE; -- for schema refreshes
CREATE SERVER server_name FOREIGN DATA WRAPPER multicorn
    OPTIONS (
        wrapper 'bigquery_fdw.fdw.ConstantForeignDataWrapper',
        fdw_dataset 'my_foreign_schema',
        fetch_size '10000',
        fdw_verbose 'true',             -- optional, get additional logging of what's going on
        fdw_colnames 'skip',            -- see options and meanings below
        fdw_colcount 'trim'             -- see optiona and meanings below
    );
DROP SCHEMA IF EXISTS new_schema_name;      -- good to keep these tables separate, so schema changes are easy
CREATE SCHEMA new_schema_name;
GRANT USAGE ON SCHEMA new_schema_name TO this_user; -- optional, maybe give your a non-admin user some permissions?
IMPORT FOREIGN SCHEMA 'my_foreign_schema'
    FROM SERVER server_name INTO new_schema_name;
```

## fdw_colnames

The only time this condition applies, is if you have a table that has at least two columns, whose 63-character prefixes match. An example of two such columns are:

* `long_column_name___2_________3_________4_________5_________6___1`
* `long_column_name___2_________3_________4_________5_________6___2`

These two strings are 64 characters long, having the same 63-character prefix `long_column_name___2_________3_________4_________5_________6___`. While this is an obviously canned example, some data models in Bigquery do use long names, and Bigquery itself supports 300-character column names. So at some point, you may need to deal with this issue. You have options:

* `error` - default option, will produce an error and should prevent any tables from being imported from this `import foreign schema` call.
* `trim` - trim any columns that have matching prefixes from the columns to be imported, possibly still allowing for remaining columns and tables to be imported.
* `skip` - skip importing this table entirely, possibly still allowing for remaining columns and tables to be imported.

Use `fdw_verbose 'true'` if you want to be notified of any trim / skip operations.

## fdw_colcount

The only time this condition applies, is if you have a table that has at least 1600 columns. Yes, big data models can have large column counts, and Bigquery itself supports 10000 columns. Postgres "only" supports 1600 columns, so if / when you have a table with 1600 columns, we can do a few things.


* `error` - default option, will produce an error and should prevent any tables from being imported from this operation.
* `trim` - trim any columns past column 1600 that are returned (all columns are returned in the order provided by `ordinal_position` in the Bigquery INFORMATION_SCHEMA.COLUMNS (which is the same order displayed in the web interface, same order returned by `select *` in Bigquery, etc.)
* `skip` - skip importing this table entirely, possibly still allowing for remaining columns and tables to be imported.

Use `fdw_verbose 'true'` if you want to be notified of any trim / skip operations.
