from dagster import ConfigurableIOManager, OutputContext, InputContext
from dagster_gcp import GCSResource
import json

class PartitionedGCSIOManager(ConfigurableIOManager):
    bucket: str
    client: GCSResource

    def _get_blob(self, context, gameId, partition):
        # Include the partition in the blob path
        path = "/".join(context.asset_key.path)
        path += f"/{partition}/{gameId}.json"
        return self.client.get_client().bucket(self.bucket).blob(path)
    
    def _get_blobs(self, asset_key_path, partition_key):
        path = "/".join(asset_key_path)
        path += f"/{partition_key}/"
        return self.client.get_client().bucket(self.bucket).list_blobs(prefix=path)

    def load_input(self, context: InputContext):
        # check the number of partitions we're trying to load
        game_data = {}
        context.log.info(f"Loading {len(context.asset_partition_keys)} partitions")
        context.log.info(f"Starting partition: {context.asset_partition_keys[0]}")
        context.log.info(f"Ending partition: {context.asset_partition_keys[-1]}")
        for partition_keys in context.asset_partition_keys:
            # we have multiple game data files to load for any given date,
            # lets return the gcs blob for each gameId
            blobs = self._get_blobs(context.asset_key.path,partition_keys)
            for blob in blobs:
                gameId = blob.name.split('/')[-1].split('.')[0]
                game_data[(context.asset_partition_key, gameId)] = json.loads(blob.download_as_string())
        return game_data
    
    def handle_output(self, context: OutputContext, obj):
        # the object is a dictionary where the key is (date, gameId), and the json of game data
        # we need to upload each game data as a json file with the path being
        # the date/gameId
        for (partition, gameId), gameData in obj.items():
            blob = self._get_blob(context, gameId, partition)
            blob.upload_from_string(json.dumps(gameData))