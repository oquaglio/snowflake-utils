# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
# Dependencies:
#   pip install snowflake-connector-python
#
# Usage:
# 	python script.py --json-file "params.json"
#
# params.json:

# {
#     "account": "blah.privatelink",
#     "user": "USER",
#     "role": "ROLE",
#     "private_key_file": "path_to_key",
#     "private_key_file_pwd": "your_password",
#     "warehouse": "WAREHOUSE",
#     "database": "DATABASE",
#     "schema": "SCHEMA",
#     "ocsp_fail_open": false
# }

from os import environ, path
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_der_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
#import getpass
import logging
import snowflake.connector

def test_snowflake_connectivity_using_connector(conn_params):

    password = None if conn_params['private_key_file_pwd'] == "" else conn_params['private_key_file_pwd']

    print(f"Testing with key: {conn_params['private_key_file']}")

    if password is not None:
        print(f"First char of KEY_PASSWORD is: {password[0]}")

    with open(conn_params['private_key_file'], "rb") as key_file:
        private_key_data = key_file.read()

    print("PCKS#8 Key Header:")
    private_key_string = private_key_data.decode("utf-8")
    print(private_key_string.split("\n")[0])

    with open(conn_params['private_key_file'], "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            private_key_data,
            password=password.encode() if password else None,
            backend=default_backend(),
        )

    # Convert the private key to PEM format if it's not already in PEM
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        #format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Ensure 'pem' is still bytes
    assert isinstance(pem, bytes), "Private key must be bytes"

    print("Converted to PEM [OK]")
    pem_string = pem.decode("utf-8")
    lines = pem_string.split("\n")
    # Print the first line
    print("First line of PEM: ", lines[0])
    # Print the last line - make sure to handle cases where the last element might be an empty string due to a trailing newline
    print("Last line of PEM: ", lines[-2] if lines[-1] == "" else lines[-1])

    # Create a connection
    print("Creating conn...")
    conn = snowflake.connector.connect(**conn_params)    
    print("Created conn [OK]")

    print("Getting cursor")
    cursor = conn.cursor()

    print("Connecting")
    try:
        print("Running SQL...")
        cursor.execute("SELECT current_user;")
        one_row = cursor.fetchone()
        print(f"Successfully ran query! Result: {one_row[0]}")
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {str(e)}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":

	import argparse
	import json

	# Create the parser
	parser = argparse.ArgumentParser(description="Read JSON input from a file.")
	# Add an argument to accept a file path
	parser.add_argument('--json-file', type=str, required=True, help='File path for JSON data.')

	# Parse the arguments
	args = parser.parse_args()

	# Read JSON data from a file and convert to Python dictionary
	try:
		with open(args.json_file, 'r') as file:
			conn_params = json.load(file)
			print("Connection Parameters:", conn_params)
	except (FileNotFoundError, json.JSONDecodeError) as e:
		print("Error reading from file:", e)

    test_snowflake_connectivity_using_connector(conn_params)