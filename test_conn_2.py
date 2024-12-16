# How to invoke:
# python script_name.py --account your_account_id --user your_username --role your_role \
# --private_key_file /path/to/private_key.p8 --private_key_file_pwd optional_password \
# --warehouse your_warehouse --database your_database --schema your_schema



import argparse
import logging
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import re

def is_key_encrypted(key_data):
    """Check if the provided key data indicates that the key is encrypted."""
    if bool(re.search(br"\s*ENCRYPTED\s*", key_data)):
        print("Match found in key data")
        return True
    else:
        print("No match found in key data")
        return False

def load_private_key(key_path, password):
    """Loads a private key, handling encrypted and unencrypted keys."""
    try:
        with open(key_path, "rb") as key_file:
            key_data = key_file.read()
            print(repr(key_data[:100]))

        encrypted = is_key_encrypted(key_data)
        if encrypted:
            if not password:
                raise ValueError("Encrypted key provided but no password was given.")
            password_bytes = password.encode()
        else:
            if password:
                logging.warning("Password was provided but the key is not encrypted. Ignoring the password.")
            password_bytes = None

        private_key = load_pem_private_key(key_data, password=password_bytes, backend=default_backend())
        return private_key, encrypted
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        raise

def test_snowflake_connectivity_using_connector(conn_params):
    try:
        private_key, encrypted = load_private_key(conn_params['private_key_file'], conn_params.get('private_key_file_pwd', ''))
        conn_params.update({"private_key": private_key})
        logging.info(f"Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=conn_params['account'],
            user=conn_params['user'],
            role=conn_params['role'],
            private_key=private_key,
            warehouse=conn_params['warehouse'],
            database=conn_params['database'],
            schema=conn_params['schema']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_user;")
        one_row = cursor.fetchone()
        print(f"Successfully connected as: {one_row[0]}")
    except Exception as e:
        logging.error(f"Error during Snowflake operation: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Connect to Snowflake using command line parameters.")
    parser.add_argument("--account", type=str, required=True, help="Snowflake account")
    parser.add_argument("--user", type=str, required=True, help="Snowflake user")
    parser.add_argument("--role", type=str, required=True, help="Snowflake role")
    parser.add_argument("--private_key_file", type=str, required=True, help="Path to the private key file")
    parser.add_argument("--private_key_file_pwd", type=str, help="Password for the encrypted private key, if any")
    parser.add_argument("--warehouse", type=str, required=True, help="Snowflake warehouse")
    parser.add_argument("--database", type=str, required=True, help="Snowflake database")
    parser.add_argument("--schema", type=str, required=True, help="Snowflake schema")

    args = parser.parse_args()

    conn_params = vars(args)
    test_snowflake_connectivity_using_connector(conn_params)
