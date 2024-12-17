# https://www.astronomer.io/docs/learn/airflow-snowflake/

# How to use:
#
# python format_private_key_script.py /path/to/your/rsa_key.pem

# This script formats a private key so we can upload it to Secrets Manager
# for Airflow to use as a variable or connection.


import argparse

def format_private_key(private_key_path):
    """Reads a private key file and formats it to escape newlines."""
    try:
        with open(private_key_path, 'r') as key_file:
            private_key = key_file.read()
        return private_key.replace('\n', '\\n')
    except IOError as e:
        return f"Error opening file: {e}"

def main():
    parser = argparse.ArgumentParser(description="Format a private key file by escaping newlines.")
    parser.add_argument("private_key_path", type=str, help="Path to the private key file.")

    args = parser.parse_args()

    formatted_key = format_private_key(args.private_key_path)
    print(formatted_key)

if __name__ == "__main__":
    main()
