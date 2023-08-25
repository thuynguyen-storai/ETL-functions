import os
from azure.storage.blob import ContainerClient


class BlobHandler:
    def __init__(self):
        blob_connection_string = BlobHandler.read_config("BLOB_CONNECTION_STRING")
        source_container = BlobHandler.read_config("BLOB_SOURCE_CONTAINER")
        dest_container = BlobHandler.read_config("BLOB_DEST_CONTAINER")

        self.source_container_client = ContainerClient.from_connection_string(blob_connection_string, source_container)
        self.dest_container_client = ContainerClient.from_connection_string(blob_connection_string, dest_container)

    def close(self):
        self.source_container_client.close()
        self.dest_container_client.close()

    def get_blobs(self):
        for blob_properties in self.source_container_client.list_blobs():
            if blob_properties.name.endswith(".gpg") or blob_properties.size >= 50000000:
                continue

            new_file_name = blob_properties.name + '.gpg'
            with \
                    self.source_container_client.get_blob_client(blob_properties) as source_blob_client, \
                    self.dest_container_client.get_blob_client(new_file_name) as dest_blob_client:
                yield source_blob_client, dest_blob_client

    @staticmethod
    def read_config(config_name: str):
        config_value = os.getenv(config_name)
        if not config_value:
            raise Exception(f"{config_name} is missing or have not declared")

        return config_value
