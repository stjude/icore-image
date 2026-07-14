#!/usr/bin/env python3
"""Create a container in the local Azurite instance and print a SAS URL.

Paste the printed URL into iCore's "SAS URL" field (IMAGINE Workflow, Image
Deidentification / Export, or Secure Data Transfer) to upload to the Azurite
started by ``docker-compose.yml`` in this directory. Run with the project venv:

    uv run python testing/azurite/sas_url.py            # print a SAS URL
    uv run python testing/azurite/sas_url.py --list     # list uploaded blobs

The account name/key are Azurite's fixed development credentials (public, not a
secret) — the same ones the pytest ``AzuriteServer`` fixture uses.
"""

import argparse
from datetime import datetime, timedelta, timezone

from azure.storage.blob import (
    BlobServiceClient,
    ContainerSasPermissions,
    generate_container_sas,
)

ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


def _client(port: int) -> BlobServiceClient:
    endpoint = f"http://127.0.0.1:{port}/{ACCOUNT_NAME}"
    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"BlobEndpoint={endpoint};"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--container",
        default="icore-export",
        help="Container name (default: icore-export)",
    )
    parser.add_argument(
        "--port", type=int, default=10000, help="Azurite blob port (default: 10000)"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="SAS token lifetime in hours (default: 24)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List blobs currently in the container and exit (verify an export)",
    )
    args = parser.parse_args()

    client = _client(args.port)

    if args.list:
        container_client = client.get_container_client(args.container)
        names = [blob.name for blob in container_client.list_blobs()]
        if names:
            print("\n".join(names))
        else:
            print(f"(no blobs in container '{args.container}')")
        return

    # Create the container if it doesn't already exist.
    try:
        client.create_container(args.container)
    except Exception:
        pass

    sas_token = generate_container_sas(
        account_name=ACCOUNT_NAME,
        container_name=args.container,
        account_key=ACCOUNT_KEY,
        permission=ContainerSasPermissions(
            read=True, write=True, delete=True, list=True
        ),
        expiry=datetime.now(timezone.utc) + timedelta(hours=args.hours),
    )
    print(f"http://127.0.0.1:{args.port}/{ACCOUNT_NAME}/{args.container}?{sas_token}")


if __name__ == "__main__":
    main()
