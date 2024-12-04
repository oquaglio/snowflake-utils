"""
Script to retrieve plain text secrets from AWS Secrets Manager.

This script fetches plain text values for `snowflake_private_key` and `snowflake_private_key_passphrase`,
directly from AWS Secrets Manager, logs them, and demonstrates how to handle them securely.

Usage:
    python script_name.py

Dependencies:
    - Python 3.x
    - `boto3` for accessing AWS Secrets Manager
    - `logging` for logging information and errors
"""

import logging
import argparse
import boto3
from botocore.exceptions import ClientError
import base64

# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to get secret from AWS Secrets Manager
def get_secret(secret_name):
    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(f"Unable to retrieve secret {secret_name}: {e}")
        raise e
    else:
        # Decrypts secret using the associated KMS CMK
        # Depending on whether the secret is a string or binary, one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            #return get_secret_value_response['SecretString']
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretString'])
            return decoded_binary_secret.decode('utf-8')
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret.decode('utf-8')

# Retrieve secrets and log them
try:
    # Names of the secrets as they are in the Secrets Manager
    snowflake_private_key = get_secret('airflow/variables/snowflake_private_key')
    snowflake_private_key_passphrase = get_secret('airflow/variables/snowflake_private_key_passphrase')

    # Log the retrieved values
    logging.info(f"Retrieved snowflake_private_key: {snowflake_private_key}")
    logging.info(f"Retrieved snowflake_private_key_passphrase: {snowflake_private_key_passphrase}")

except Exception as e:
    logging.error(f"An error occurred: {e}")
