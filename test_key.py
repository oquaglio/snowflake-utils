from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import argparse
import json

def load_and_test_private_key(conn_params):
    password = None if conn_params["private_key_file_pwd"] == "" else conn_params["private_key_file_pwd"]
    private_key_path = conn_params.get("private_key_file")

    if not private_key_path:
        raise ValueError("Private key file path is not specified.")

    print(f"Testing with key: {conn_params['private_key_file']}")

    try:
        with open(private_key_path, "rb") as key_file:
            private_key_data = key_file.read()

        # Check if the private key is encrypted
        if b"ENCRYPTED" in private_key_data:
            if not password:
                raise ValueError("Password required but not provided for encrypted private key.")
            password_bytes = password.encode()
        else:
            password_bytes = None

        # Load the private key
        private_key = load_pem_private_key(
            private_key_data,
            password=password_bytes,
            backend=default_backend(),
        )
        print("Private key loaded successfully.")

        # Optionally, convert to PEM and print the header to check the format
        pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        print("Private key is in PEM format:")
        pem_lines = pem.decode("utf-8").split("\n")
        print(pem_lines[0])  # Print the first line of the PEM file

    except Exception as e:
        print(f"Error loading private key: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load and test private key from JSON config.")
    parser.add_argument("--json-file", type=str, required=True, help="File path for JSON data containing private key parameters.")
    args = parser.parse_args()

    try:
        with open(args.json_file, "r") as file:
            conn_params = json.load(file)
            print("Loaded Connection Parameters.")
        load_and_test_pr
