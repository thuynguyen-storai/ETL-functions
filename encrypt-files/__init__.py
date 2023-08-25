"""
A simple single-key encryption and decryption operation.
It does not match real-world scenario, though
"""

import logging

import azure.functions as func

from .GPGWrapper import GPGWrapper
from .BlobHandler import BlobHandler


async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Invoke file encrypt")

    try:
        blob_handler = BlobHandler()

        storai_public_key_fingerprint = GPGWrapper.import_from_config_name("STORAI_PUBLIC_KEY")
        logging.debug("Import key STORAI_PUBLIC_KEY")

        customer_private_key_fingerprint = GPGWrapper.import_from_config_name("CUSTOMER_PRIVATE_KEY")
        logging.debug("Import key CUSTOMER_PRIVATE_KEY")

        for source_blob_client, dest_blob_client in blob_handler.get_blobs():
            if source_blob_client.blob_name.endswith(".gpg"):
                continue

            encrypted_data = GPGWrapper.encrypt(
                source_blob_client.download_blob(),
                storai_public_key_fingerprint,
                customer_private_key_fingerprint)

            dest_blob_client.upload_blob(encrypted_data)

        return func.HttpResponse("Operation succeed", status_code=200)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse("Internal Server Error", status_code=500)

    finally:
        blob_handler.close()
