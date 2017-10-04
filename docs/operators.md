# Supported operators

## Comparison operators

| Operator | Name | Supported in bigquery_fdw? |
|-----|-----|-----|
| = | Equal | ✓ |
| < | Less than | ✓ |
| > | Greater than | ✓ |
| <= | Less than or equal to | ✓ |
| >= | Greater than or equal to | ✓ |
| !=, <> | Not equal | ✓ |
| [NOT] LIKE | Value does [not] match the pattern specified | ✓ |
| [NOT] BETWEEN | Value is [not] within the range specified | ✗ |
| [NOT] IN | Value is [not] in the set of values specified	 | ✗ |
| IS [NOT] NULL | Value is [not] NULL | ✗ |
| IS [NOT] TRUE | Value is [not] TRUE. | ✗ |
| IS [NOT] FALSE | Value is [not] FALSE. | ✗ |

## Logical operators

| Operator | Name | Supported in bigquery_fdw? |
|-----|-----|-----|
| NOT | Logical NOT | ✗ |
| AND | Logical AND | ✓ |
| OR | Logical OR | ✗ |

## External links

 - [BigQuery functions and operators](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators)
