from dagster import (
    asset,
    AssetIn,
    AutoMaterializePolicy,
    BackfillPolicy
)
from ..partitions import nhl_season_partition, PreviousSeasonCurrentSeasonPartitionMapping


@asset(
    partitions_def=nhl_season_partition,
    io_manager_key="nhl_season_partitioned_bigquery_io_manager",
    group_name="nhl_ingest",
    compute_kind="Python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={"nhl_game_data_by_season": AssetIn("nhl_game_data_by_season", partition_mapping=PreviousSeasonCurrentSeasonPartitionMapping())},
    key_prefix=["nhl_warehouse"]
)
def nhl_game_summary(context, nhl_game_data_by_season):
    