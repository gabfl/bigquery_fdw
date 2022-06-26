# Test BigQuery client

bigquery_fdw contains the utility `bq_client_test` to test a query against the BigQuery database.

## Usage

```bash
# Provide authentication credentials to BigQuery by setting the environment variable GOOGLE_APPLICATION_CREDENTIALS
# See https://cloud.google.com/docs/authentication/getting-started
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

bq_client_test --query "SELECT count(*) FROM my_dataset.my_table"
```

## Example

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
$ bq_client_test --query "SELECT count(*) FROM my_dataset.my_table"

 * BigQuery client instance:
<google.cloud.bigquery.client.Client object at 0x10c863e48>
 * Query instance:
<google.cloud.bigquery.job.QueryJob object at 0x10c863c18>
 * Query results:
Row((21215,), {'f0_': 0})
```

You can also test against BigQuery public datasets with:
```bash
bq_client_test --query "SELECT * FROM bigquery-public-data.usa_names.usa_1910_current WHERE year = 2017 AND number > 1000 LIMIT 5;"
```
