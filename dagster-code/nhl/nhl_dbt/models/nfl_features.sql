WITH
  p AS (
  SELECT
    DISTINCT CAST(player_id AS STRING) AS player_id,
    STATUS AS player_status,
    WEIGHT AS player_weight,
    HEIGHT AS player_height,
    CAST(LEFT(BIRTH_DATE, 4) AS INT64) AS player_birth_year,
  IF
    (position = 'QB', 1, 0) AS player_qb,
  IF
    (position = 'RB', 1, 0) AS player_rb,
  IF
    (position = 'WR', 1, 0) AS player_wr,
  IF
    (position = 'TE', 1, 0) AS player_te,
    DRAFT_SELECTION AS player_draft_select,
    IFNULL(DRAFT_YEAR, DEBUT_YEAR) AS player_draft_year
  FROM
    `corellian-engineering-co.NHLData.espn_nfl_player`
  WHERE
    position IN ('QB',
      'RB',
      'WR',
      'TE')
    AND IFNULL(DRAFT_YEAR, DEBUT_YEAR) IS NOT NULL),
  s AS (
  SELECT
    CAST(player_id AS STRING) AS player_id,
    CAST(season AS INT64) AS season,
  IF
    ((RUSHING_TEAMGAMESPLAYED IS NULL
        OR RUSHING_TEAMGAMESPLAYED = 0)
      AND (GENERAL_GAMESPLAYED > 0), GENERAL_GAMESPLAYED, RUSHING_TEAMGAMESPLAYED) AS rushing_games,
    RUSHING_RUSHINGATTEMPTS AS rushing_attempts,
    RUSHING_RUSHINGTOUCHDOWNS AS rushing_td,
    RUSHING_RUSHINGYARDS AS rushing_yd,
    RUSHING_RUSHINGFUMBLES AS rushing_fum,
  IF
    ((RECEIVING_TEAMGAMESPLAYED IS NULL
        OR RECEIVING_TEAMGAMESPLAYED = 0)
      AND (GENERAL_GAMESPLAYED > 0), GENERAL_GAMESPLAYED, RECEIVING_TEAMGAMESPLAYED) AS receiving_games,
    RECEIVING_RECEIVINGTARGETS AS receiving_targets,
    RECEIVING_RECEPTIONS AS receiving_receptions,
    RECEIVING_RECEIVINGTOUCHDOWNS AS receiving_td,
    RECEIVING_RECEIVINGYARDS AS receiving_yd,
    RECEIVING_RECEIVINGFUMBLES AS receiving_fum,
  IF
    ((PASSING_TEAMGAMESPLAYED IS NULL
        OR PASSING_TEAMGAMESPLAYED = 0)
      AND (GENERAL_GAMESPLAYED > 0), GENERAL_GAMESPLAYED, PASSING_TEAMGAMESPLAYED) AS passing_games,
    PASSING_PASSINGATTEMPTS AS passing_attempts,
    PASSING_COMPLETIONS AS passing_completions,
    PASSING_PASSINGTOUCHDOWNS AS passing_td,
    PASSING_PASSINGYARDS AS passing_yd,
    PASSING_INTERCEPTIONS AS passing_int,
    PASSING_PASSINGFUMBLES AS passing_fum
  FROM
    `corellian-engineering-co.NHLData.espn_nfl_player_stats_by_season`
  WHERE
    CAST(player_id AS STRING) IN (
    SELECT
      player_id
    FROM
      p)),
  g AS (
  SELECT
    s.player_id,
    s.season,
    SUM(s.rushing_games) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_games,
    SUM(rushing_games) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_games,
    SUM(s.rushing_attempts) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rush_attempts,
    SUM(s.rushing_attempts) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rush_attempts,
    SUM(s.rushing_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rush_td,
    SUM(s.rushing_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rush_td,
    SUM(s.rushing_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rush_yd,
    SUM(s.rushing_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rush_yd,
    SUM(s.rushing_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rush_fum,
    SUM(s.rushing_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rush_fum,
    SUM(s.receiving_targets) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rec_targets,
    SUM(s.receiving_targets) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rec_targets,
    SUM(s.receiving_receptions) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rec_receptions,
    SUM(s.receiving_receptions) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rec_receptions,
    SUM(s.receiving_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rec_td,
    SUM(s.receiving_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rec_td,
    SUM(s.receiving_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rec_yd,
    SUM(s.receiving_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rec_yd,
    SUM(s.receiving_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_rec_fum,
    SUM(s.receiving_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_rec_fum,
    SUM(s.passing_attempts) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_attempts,
    SUM(s.passing_attempts) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_attempts,
    SUM(s.passing_completions) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_completions,
    SUM(s.passing_completions) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_completions,
    SUM(s.passing_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_td,
    SUM(s.passing_td) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_td,
    SUM(s.passing_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_yd,
    SUM(s.passing_yd) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_yd,
    SUM(s.passing_int) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_int,
    SUM(s.passing_int) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_int,
    SUM(s.passing_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trail_two_pass_fum,
    SUM(s.passing_fum) OVER (PARTITION BY s.player_id ORDER BY s.season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS trail_three_pass_fum
  FROM
    s)
SELECT
  player_id,
  s.season,
  s.season + 1 AS season_next,
  -- player metrics
  s.season - p.player_birth_year AS season_age,
  s.season - p.player_draft_year AS season_experience,
  p.player_weight,
  p.player_height,
  p.player_qb,
  p.player_rb,
  p.player_wr,
  p.player_te,
  p.player_draft_select,
  -- season stats
  IFNULL(SAFE_DIVIDE(s.rushing_attempts, s.rushing_games), 0) AS rushing_per_attempts,
  IFNULL(SAFE_DIVIDE(s.rushing_td, s.rushing_games), 0) AS rushing_per_td,
  IFNULL(SAFE_DIVIDE(s.rushing_yd, s.rushing_games), 0) AS rushing_per_yd,
  IFNULL(SAFE_DIVIDE(s.rushing_fum, s.rushing_games), 0) AS rushing_per_fum,
  IFNULL(SAFE_DIVIDE(s.receiving_targets, s.receiving_games), 0) AS receiving_per_targets,
  IFNULL(SAFE_DIVIDE(s.receiving_receptions, s.receiving_games), 0) AS receiving_per_rec,
  IFNULL(SAFE_DIVIDE(s.receiving_td, s.receiving_games), 0) AS receiving_per_td,
  IFNULL(SAFE_DIVIDE(s.receiving_yd, s.receiving_games), 0) AS receiving_per_yd,
  IFNULL(SAFE_DIVIDE(s.receiving_fum, s.receiving_games), 0) AS receiving_per_fum,
  IFNULL(SAFE_DIVIDE(s.passing_attempts, s.passing_games), 0) AS passing_per_attempts,
  IFNULL(SAFE_DIVIDE(s.passing_completions, s.passing_games), 0) AS passing_per_completions,
  IFNULL(SAFE_DIVIDE(s.passing_td, s.passing_games), 0) AS passing_per_td,
  IFNULL(SAFE_DIVIDE(s.passing_yd, s.passing_games), 0) AS passing_per_yd,
  IFNULL(SAFE_DIVIDE(s.passing_int, s.passing_games), 0) AS passing_per_int,
  IFNULL(SAFE_DIVIDE(s.passing_fum, s.passing_games), 0) AS passing_per_fum,
  -- trailing
  IFNULL(SAFE_DIVIDE(g.trail_two_rush_attempts, g.trail_two_games), 0) AS trail_two_per_rush_attempts,
  IFNULL(SAFE_DIVIDE(g.trail_three_rush_attempts, g.trail_three_games), 0) AS trail_three_per_rush_attempts,
  IFNULL(SAFE_DIVIDE(g.trail_two_rush_td, g.trail_two_games), 0) AS trail_two_per_rush_td,
  IFNULL(SAFE_DIVIDE(g.trail_three_rush_td, g.trail_three_games), 0) AS trail_three_per_rush_td,
  IFNULL(SAFE_DIVIDE(g.trail_two_rush_yd, g.trail_two_games), 0) AS trail_two_per_rush_yd,
  IFNULL(SAFE_DIVIDE(g.trail_three_rush_yd, g.trail_three_games), 0) AS trail_three_per_rush_yd,
  IFNULL(SAFE_DIVIDE(g.trail_two_rush_fum, g.trail_two_games), 0) AS trail_two_per_rush_fum,
  IFNULL(SAFE_DIVIDE(g.trail_three_rush_fum, g.trail_three_games), 0) AS trail_three_per_rush_fum,
  IFNULL(SAFE_DIVIDE(g.trail_two_rec_targets, g.trail_two_games), 0) AS trail_two_per_rec_targets,
  IFNULL(SAFE_DIVIDE(g.trail_three_rec_targets, g.trail_three_games), 0) AS trail_three_per_rec_targets,
  IFNULL(SAFE_DIVIDE(g.trail_two_rec_receptions, g.trail_two_games), 0) AS trail_two_per_rec_receptions,
  IFNULL(SAFE_DIVIDE(g.trail_three_rec_receptions, g.trail_three_games), 0) AS trail_three_per_rec_receptions,
  IFNULL(SAFE_DIVIDE(g.trail_two_rec_td, g.trail_two_games), 0) AS trail_two_per_rec_td,
  IFNULL(SAFE_DIVIDE(g.trail_three_rec_td, g.trail_three_games), 0) AS trail_three_per_rec_td,
  IFNULL(SAFE_DIVIDE(g.trail_two_rec_yd, g.trail_two_games), 0) AS trail_two_per_rec_yd,
  IFNULL(SAFE_DIVIDE(g.trail_three_rec_yd, g.trail_three_games), 0) AS trail_three_per_rec_yd,
  IFNULL(SAFE_DIVIDE(g.trail_two_rec_fum, g.trail_two_games), 0) AS trail_two_per_rec_fum,
  IFNULL(SAFE_DIVIDE(g.trail_three_rec_fum, g.trail_three_games), 0) AS trail_three_per_rec_fum,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_attempts, g.trail_two_games), 0) AS trail_two_per_pass_attempts,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_attempts, g.trail_three_games), 0) AS trail_three_per_pass_attempts,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_completions, g.trail_two_games), 0) AS trail_two_per_pass_completions,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_completions, g.trail_three_games), 0) AS trail_three_per_pass_completions,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_td, g.trail_two_games), 0) AS trail_two_per_pass_td,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_td, g.trail_three_games), 0) AS trail_three_per_pass_td,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_yd, g.trail_two_games), 0) AS trail_two_per_pass_yd,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_yd, g.trail_three_games), 0) AS trail_three_per_pass_yd,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_int, g.trail_two_games), 0) AS trail_two_per_pass_int,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_int, g.trail_three_games), 0) AS trail_three_per_pass_int,
  IFNULL(SAFE_DIVIDE(g.trail_two_pass_fum, g.trail_two_games), 0) AS trail_two_per_pass_fum,
  IFNULL(SAFE_DIVIDE(g.trail_three_pass_fum, g.trail_three_games), 0) AS trail_three_per_pass_fum
FROM
  s
LEFT JOIN
  p
USING
  (player_id)
LEFT JOIN
  g
USING
  (player_id,
    season)
WHERE
  s.season - p.player_draft_year >= 0