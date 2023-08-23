from typing import Any, Mapping

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator


class NHLDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        asset_key = super().get_asset_key(dbt_resource_props)
        # take the prefix from the asset key and add it
        # as a string to the asset key with a _
        prefix = asset_key.path_components[0]
        asset_key = AssetKey(f"{prefix}_{asset_key.path_components[1]}")
        return asset_key
