# Snowflake Utils

## Python Dependencies

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector

pip install snowflake-connector-python


## test_conn.py Usage:

python test_conn.py --json-file "test_conn.json"

test_conn_params.json:

```JSON
{
    "account": "blah.privatelink",
    "user": "USER",
    "role": "ROLE",
    "private_key_file": "path_to_key",
    "private_key_file_pwd": "your_password",
    "warehouse": "WAREHOUSE",
    "database": "DATABASE",
    "schema": "SCHEMA",
    "ocsp_fail_open": false
}
```

## test_key.py Usage:

python test_conn.py --json-file "test_conn.json"


test_key_params.json:

```JSON
{
    "private_key_file": "path_to_key",
    "private_key_file_pwd": "your_password"
}
```