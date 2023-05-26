from dagster import build_asset_reconciliation_sensor, AssetSelection

clean_assets_sensor = build_asset_reconciliation_sensor(
    AssetSelection.groups("clean"),
    name="clean_assets_sensor",
    description="Sensor to keep clean assets up to date with the raw assets",
)

morph_assets_sensor = build_asset_reconciliation_sensor(
    AssetSelection.groups("morph"),
    name="clean_assets_sensor",
    description="Sensor to keep clean assets up to date with the raw assets",
)