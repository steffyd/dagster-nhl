from typing import Any, Mapping

from dagster import AssetKey, MetadataValue
from dagster_dbt import DagsterDbtTranslator


class NHLDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        asset_key = super().get_asset_key(dbt_resource_props).path
        # if the asset key has more than one part, we want to
        # add the first part as a prefix to the second part
        ret_asset_key = AssetKey(asset_key)
        if len(asset_key) > 1:
            # take the prefix from the asset key and add it
            # as a string to the asset key with a _ separator
            prefix = asset_key[0]
            ret_asset_key = AssetKey(f"{prefix}_{asset_key[1]}")
        return ret_asset_key
