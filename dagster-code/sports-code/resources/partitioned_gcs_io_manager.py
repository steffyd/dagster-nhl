from dagster import ConfigurableIOManager, OutputContext
from dagster_gcp import GCSResource
import json

class PartitionedGCSIOManager(ConfigurableIOManager):
    bucket: str
    client: GCSResource

    def _get_blob(self, context, gameId, partition):
        # Include the partition in the blob path
        path = "/".join(context.asset_key.path)
        path += f"/{partition}/{gameId}.json"
        context.log.info(f'Uploading game data to {path}')
        return self.client.get_client().bucket(self.bucket).blob(path)
    
    def _get_blobs(self, context):
        path = "/".join(context.asset_key.path)
        return self.client.get_client().bucket(self.bucket).list_blobs(prefix=path)

    def load_input(self, context):
        # we have multiple game data files to load for any given date,
        # so we need to load all of them and return them as a dictionary
        # of gameId to game data
        blobs = self._get_blobs(context)
        game_data = {}
        for blob in blobs:
            gameId = blob.name.split('/')[-1].split('.')[0]
            game_data[gameId] = json.loads(blob.download_as_string())
        return game_data
    
    def handle_output(self, context: OutputContext, obj):
        context.log.info(context.asset_key.path)
        # the object is a dictionary where the key is (date, gameId), and the json of game data
        # we need to upload each game data as a json file with the path being
        # the date/gameId
        for (partition, gameId), gameData in obj.items():
            blob = self._get_blob(context, gameId, partition)
            #blob.upload_from_string(json.dumps(gameData))