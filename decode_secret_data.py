"""
Script to decode base64-encoded key and password from a JSON file.

This script reads a JSON file containing base64-encoded values for `key` and `password`,
decodes them, and logs the decoded values.

Usage:
    python script_name.py --file-path /path/to/secret_data.json

Arguments:
    --file-path : The path to the JSON file containing `snowflake_private_key` and `snowflake_private_key_password`.

Example:
    python script_name.py --file-path ./secret_data.json

Dependencies:
    - Python 3.x
    - `argparse` for parsing command-line arguments
    - `json` for reading JSON files
    - `base64` for decoding base64-encoded strings
    - `logging` for logging decoded values and errors
"""

import json
import base64
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)

# Argument parser to accept the path to the JSON file
parser = argparse.ArgumentParser(description="Decode base64-encoded snowflake_private_key and snowflake_private_key_password from a JSON file.")
parser.add_argument(
    "--file-path",
    type=str,
    required=True,
    help="Path to the secret_data.json file."
)

args = parser.parse_args()

# Load the JSON file from the provided path
try:
    with open(args.file_path, 'r') as file:
        secret_data = json.load(file)

    # Extract and decode the base64-encoded values
    snowflake_private_key_encoded = secret_data['snowflake_private_key']
    snowflake_private_key_password_encoded = secret_data['snowflake_private_key_password']

    # Decode the base64-encoded strings
    snowflake_private_key_decoded = base64.b64decode(snowflake_private_key_encoded).decode('utf-8')
    snowflake_private_key_password_decoded = base64.b64decode(snowflake_private_key_password_encoded).decode('utf-8')

    # Log the decoded values
    logging.info(f"Decoded snowflake_private_key: {snowflake_private_key_decoded}")
    logging.info(f"Decoded snowflake_private_key_password: {snowflake_private_key_password_decoded}")

except FileNotFoundError:
    logging.error(f"File not found: {args.file_path}")
except KeyError as e:
    logging.error(f"Missing key in JSON data: {e}")
except (base64.binascii.Error, UnicodeDecodeError) as e:
    logging.error(f"Error decoding base64 data: {e}")
except json.JSONDecodeError as e:
    logging.error(f"Invalid JSON format: {e}")
