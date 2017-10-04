# Private Key storage recommendations

The private Json key needs to be stored in a file accessible to the PostgreSQL client.

If the key is not accessible, PostgreSQL client will throw the following error:

```
ERROR:  Error in python: PermissionError
DETAIL:  [Errno 13] Permission denied: '/path/to/key.json'
```

It is recommended to store the key in a dedicated folder `/opt/bigquery_fdw`:

```bash
# Create directory
mkdir -p /opt/bigquery_fdw

# Copy key in directory
cp /current/path/to/key.json /opt/bigquery_fdw/

# Set correct permissions
chown -R postgres.postgres /opt/bigquery_fdw
chmod -R 700 /opt/bigquery_fdw
```
