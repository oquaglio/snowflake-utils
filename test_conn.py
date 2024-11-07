import argparse
import json
import logging
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend


def test_snowflake_connectivity_using_connector(conn_params):
    private_key_path = conn_params.get("private_key_file")
    password = conn_params.get("private_key_file_pwd", None)

    try:
        with open(private_key_path, "rb") as key_file:
            private_key_data = key_file.read()

        private_key = load_pem_private_key(
            private_key_data,
            password=password.encode() if password else None,
            backend=default_backend(),
        )

        # Connect to Snowflake using the loaded private key and connection parameters
        conn_params.update({"private_key": private_key})

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
