import argparse
import json
import logging
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend


def is_key_encrypted(key_data):
    """Check if the provided key data indicates that the key is encrypted."""
    return b"ENCRYPTED" in key_data


def load_private_key(key_path, password):
    """Loads a private key, correctly handling encrypted and unencrypted keys."""
    try:
        with open(key_path, "rb") as key_file:
            key_data = key_file.read()

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

        return private_key
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        raise


def test_snowflake_connectivity_using_connector(conn_params):
    private_key_path = conn_params.get("private_key_file")
    password = conn_params.get("private_key_file_pwd", "")

    # Load the private key without causing issues with non-encrypted keys
    private_key = load_private_key(private_key_path, password)
    conn_params.update({"private_key": private_key})

    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**conn_params)
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
