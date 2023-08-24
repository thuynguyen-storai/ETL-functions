import logging
import os
import re
from datetime import datetime

import azure.functions as func
from azure.storage.blob.aio import ContainerClient


def system_has_valid_configs():
    configuration_names = [
        "SOURCE_BLOB_CONNECTION_STRING",
        "SOURCE_CONTAINER",
        "DEST_BLOB_CONNECTION_STRING",
        "DEST_CONTAINER",
        "ARCHIVE_OFFSET_DAY"
    ]

    for config_name in configuration_names:
        if not os.getenv(config_name):
            logging.error(f"Missing configuration: {config_name}")
            return False

    return True


async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if not system_has_valid_configs():
        return func.HttpResponse("Invalid system configuration", status_code=500)

    try:
        async with \
                ContainerClient.from_connection_string(
                    os.getenv("SOURCE_BLOB_CONNECTION_STRING"),
                    os.getenv("SOURCE_CONTAINER")
                ) as source_container, \
                ContainerClient.from_connection_string(
                    os.getenv("DEST_BLOB_CONNECTION_STRING"),
                    os.getenv("DEST_CONTAINER")
                ) as dest_container:

            file_name_regex_pattern = re.compile(r"tracking-info-(\d{2}-\d{2}-\d{4})-.*\.json")

            archive_offset_day = int(os.getenv("ARCHIVE_OFFSET_DAY"))
            today = datetime.today()

            # ! only process container's top-level files
            async for source_file_properties in source_container.list_blobs(name_starts_with="tracking-info"):
                file_regex_matches = re.match(file_name_regex_pattern, source_file_properties.name)
                file_added_date = datetime.strptime(file_regex_matches.group(1), r"%m-%d-%Y")

                if (today - file_added_date).days <= archive_offset_day:
                    logging.debug("Skip file: %s", source_file_properties.name)
                    continue

                logging.debug("Archived file: %s", source_file_properties.name)
                source_blob_client = source_container.get_blob_client(source_file_properties)
                dest_blob_client = dest_container.get_blob_client(source_file_properties)
                await dest_blob_client.upload_blob_from_url(source_blob_client.url, overwrite=True)
                await source_blob_client.delete_blob()

        logging.info("Archived success")
        return func.HttpResponse("Operation succeed", status_code=200)

    except Exception as e:
        logging.exception(e)
        return func.HttpResponse("Operation failed", status_code=500)
