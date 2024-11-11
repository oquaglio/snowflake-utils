import argparse
import json
import logging
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import re

encrypted=False

def is_key_encrypted(key_data):
    """Check if the provided key data indicates that the key is encrypted."""
    #return b"ENCRYPTED" in key_data
    #return b"ENCRYPTED" in key_data
    #return bool(re.search(br"ENCRYPTED", key_data))

    if bool(re.search(br"\s*ENCRYPTED\s*", key_data)):
        print("Match found in key data")  # Debug log to confirm match
        return True
    else:
        print("No match found in key data")
        return False



def load_private_key(key_path, password):
    """Loads a private key, correctly handling encrypted and unencrypted keys."""
    try:
        with open(key_path, "rb") as key_file:
            key_data = key_file.read()
           # print(key_data[:100])  # Print the first 100 bytes for inspection
            print(repr(key_data[:100]))  # Print the first 100 bytes with all hidden characters shown



        encrypted = is_key_encrypted(key_data)

        if encrypted:
            if not password:
                raise ValueError("Encrypted key provided but no password was given.")
            password_bytes = password.encode()
        else:
            if password:
                logging.warning(
                    "Password was provided but the key is not encrypted. Ignoring the password."
                )
            password_bytes = None  # Do not use a password for unencrypted keys

        # Load the private key
        private_key = load_pem_private_key(
            key_data,
            password=password_bytes,
            backend=default_backend(),
        )

        return private_key, encrypted  # Return the key and encrypted status
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        raise


def test_snowflake_connectivity_using_connector(conn_params):
    private_key_path = conn_params.get("private_key_file")
    password = conn_params.get("private_key_file_pwd", "")

    # Load the private key without causing issues with non-encrypted keys
    private_key, encrypted = load_private_key(private_key_path, password)
    conn_params.update({"private_key": private_key})

    logging.info(conn_params)

    logging.info(f"Connecting to Snowflake...")
    # Connect to Snowflake
    try:
        #conn = snowflake.connector.connect(**conn_params)
        if encrypted:
            logging.info(f"Private key is encrypted; conn will use provided password")
            conn = snowflake.connector.connect(
                account=conn_params['account'],
                user=conn_params['user'],
                role=conn_params['role'],
                private_key_file=conn_params['private_key_file'],
                private_key_file_pwd=conn_params['private_key_file_pwd'],
                warehouse=conn_params['warehouse'],
                database=conn_params['database'],
                schema=conn_params['schema']
                )
        else:
            logging.info(f"Private key is not encrypted; conn will not use a password")
            conn = snowflake.connector.connect(
                account=conn_params['account'],
                user=conn_params['user'],
                role=conn_params['role'],
                private_key_file=conn_params['private_key_file'],
                warehouse=conn_params['warehouse'],
                database=conn_params['database'],
                schema=conn_params['schema']
                )

        logging.info(f"Getting cursor...")
        cursor = conn.cursor()
        cursor.execute("SELECT current_user;")
        one_row = cursor.fetchone()
        print(f"Successfully connected as: {one_row[0]}")
    except Exception as e:
        logging.error(f"Error during Snowflake operation: {str(e)}")
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Connect to Snowflake using JSON configuration.")
    parser.add_argument(
        "--json-file",
        type=str,
        required=True,
        help="File path for JSON data containing connection parameters.",
    )

    args = parser.parse_args()

    try:
        with open(args.json_file, "r") as file:
            conn_params = json.load(file)
        test_snowflake_connectivity_using_connector(conn_params)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Failed to load connection parameters: {e}")