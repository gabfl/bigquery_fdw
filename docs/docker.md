# Use bigquery_fdw on docker

## Pre-requisite

You will need:
 - To clone bigquery_fdw on your compoter
 - To [create a "Service account"](service_account.md) with the [correct permissions](service_account_permissions.md)

## Clone the project and launch the docker image

```bash
git clone https://github.com/gabfl/bigquery_fdw.git

docker run \
-v ~/path/to/bigquery_fdw:/opt/bigquery_fdw \
-v ~/path/to/key.json:/opt/key/key.json \
-ti gabfl/bigquery_fdw
```

## What is in the image?

The docker images comes with:
 - PostgreSQL 10 installed
 - Multicorn installed and compiled
 - All the packages required by bigquery_fdw