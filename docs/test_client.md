# Test BigQuery client

bigquery_fdw contains the utility `bq_client_test` to test a query against the BigQuery database.

## Usage

```bash
bq_client_test --key /path/to/key.json \
               --query "SELECT count(*) FROM my_dataset.my_table"
```

## Example

```
$ bq_client_test --key /path/to/key.json \
>                --query "SELECT count(*) FROM my_dataset.my_table"

 * BigQuery client instance:
<google.cloud.bigquery.client.Client object at 0x10c863e48>
 * Query instance:
<google.cloud.bigquery.job.QueryJob object at 0x10c863c18>
 * Query results:
Row((21215,), {'f0_': 0})
```
