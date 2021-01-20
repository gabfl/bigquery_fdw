-- Create extension
CREATE EXTENSION multicorn;

-- Create server
CREATE SERVER bigquery_srv FOREIGN DATA WRAPPER multicorn
OPTIONS (
    wrapper 'bigquery_fdw.fdw.ConstantForeignDataWrapper'
);

-- Create sample table
CREATE FOREIGN TABLE usa_names (
    state text,
    gender text,
    year bigint,
    name text,
    number bigint
) SERVER bigquery_srv
OPTIONS (
    fdw_dataset 'bigquery-public-data.usa_names',
    fdw_table 'usa_1910_current'
);
