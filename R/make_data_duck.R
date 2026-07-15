## Streams hot path on DuckDB -------------------------------------------------
##
## Replaces the materialise-user×song×week-in-R pattern. Both the artist-level
## table and the acoustic weighted moments are computed straight from the stream
## parquet files, out-of-core, so the giant `user_song_per_period` target is no
## longer built.
##
## The SQL below was verified (scratchpad harness) to reproduce, to float
## precision, the previous tidytable results including:
##   - songs split across long/short files summed before weighting
##   - truncate_hashed_id() (first 9 word chars) with its id-collision merging
##   - breakdown_time(ts,'week') == strftime(to_timestamp(ts),'%Y-%W')
##   - Hmisc::wtd.mean / wtd.var(normwt = TRUE) for the acoustic moments
##
## since_ts default 1546300800 = 2019-01-01 (matches make_user_song_per_period_onefile).

#' Split the streaming file list into the two schemas (long: song_id;
#' short: media_id/media_type), as classified by the filename.
split_stream_files <- function(streaming_files) {
  list(long  = streaming_files[stringr::str_detect(streaming_files, "long")],
       short = streaming_files[stringr::str_detect(streaming_files, "short")])
}

#' Normalised-stream CTE body: hashed_id (truncated), period, song_id,
#' context_4, lt. Long and short schemas unioned into one shape.
duck_stream_cte <- function(streaming_files, since_ts, interval = "week",
                            bucket = "scoavoux") {
  fmt <- if (interval == "week") "%Y-%W" else "%Y-%m"
  f <- split_stream_files(streaming_files)
  parts <- character(0)
  common <- paste0(
    "regexp_extract(hashed_id,'^\\w{9}') AS hashed_id, ",
    "strftime(to_timestamp(ts_listen),'", fmt, "') AS period, ")
  if (length(f$long) > 0) {
    parts <- c(parts, sprintf(
      "SELECT %s song_id AS song_id, context_4, greatest(listening_time,0) AS lt
       FROM read_parquet(%s, union_by_name=true)
       WHERE is_listened = 1 AND ts_listen >= %d",
      common, duck_uri_list(f$long, bucket), since_ts))
  }
  if (length(f$short) > 0) {
    parts <- c(parts, sprintf(
      "SELECT %s media_id AS song_id, context_4, greatest(listening_time,0) AS lt
       FROM read_parquet(%s, union_by_name=true)
       WHERE is_listened = 1 AND ts_listen >= %d AND media_type = 'song'",
      common, duck_uri_list(f$short, bucket), since_ts))
  }
  paste(parts, collapse = "\n    UNION ALL\n")
}

#' user × period × artist × context_4 -> l_play, n_play, directly from streams.
#' Drop-in replacement for make_user_song_per_period + make_user_artist_per_period.
make_user_artist_per_period_duck <- function(streaming_files, users, items,
                                             artists_to_remove,
                                             interval = "week",
                                             since_ts = 1546300800L,
                                             bucket = "scoavoux",
                                             threads = NULL, memory_limit = NULL) {
  with_duck(function(con) {
    duckdb::duckdb_register(con, "users_rel",  dplyr::select(users, hashed_id))
    duckdb::duckdb_register(con, "items_rel",  dplyr::select(items, song_id, artist_id))
    duckdb::duckdb_register(con, "remove_rel", dplyr::select(artists_to_remove, artist_id))
    sql <- sprintf("
      WITH streams AS (%s),
      kept AS (
        SELECT s.hashed_id, s.period, s.song_id, s.context_4, s.lt
        FROM streams s
        SEMI JOIN users_rel u ON u.hashed_id = s.hashed_id
      )
      SELECT k.hashed_id, k.period, i.artist_id, k.context_4,
             sum(k.lt) AS l_play,
             count(*)  AS n_play
      FROM kept k
      JOIN items_rel i USING (song_id)
      ANTI JOIN remove_rel r ON r.artist_id = i.artist_id
      GROUP BY 1,2,3,4
      ORDER BY k.period",
      duck_stream_cte(streaming_files, since_ts, interval, bucket))
    res <- DBI::dbGetQuery(con, sql)
    res$period <- factor(res$period)
    res
  }, .threads = threads, .memory_limit = memory_limit)
}

#' user × period weighted mean/sd of each acoustic feature, straight from streams.
#' Drop-in replacement for compute_acoustic_diversity (reads streams, not the
#' materialised song-level table). `acoustic_features` = acoustic_features_with_pca
#' (song_id + danceability..tempo + pc1..pc4).
compute_acoustic_diversity_duck <- function(streaming_files, acoustic_features, users,
                                            interval = "week",
                                            since_ts = 1546300800L,
                                            bucket = "scoavoux",
                                            threads = NULL, memory_limit = NULL) {
  features <- setdiff(names(acoustic_features), "song_id")
  suff <- paste(sprintf("sum(l*%1$s) AS Sx_%1$s, sum(l*%1$s*%1$s) AS Sxx_%1$s", features),
                collapse = ",\n             ")
  feat_cols <- paste0("f.", features, collapse = ", ")
  finals <- paste(vapply(features, function(f) {
    mean <- sprintf("(Sx_%s/W)", f)
    var  <- sprintf("(n/(n-1.0))*(Sxx_%1$s/W - %2$s*%2$s)", f, mean)
    sprintf("%2$s AS %1$s_mean, sqrt(greatest(%3$s,0)) AS %1$s_sd", f, mean, var)
  }, character(1)), collapse = ",\n         ")

  with_duck(function(con) {
    duckdb::duckdb_register(con, "users_rel", dplyr::select(users, hashed_id))
    duckdb::duckdb_register(con, "feat_rel",  acoustic_features)
    sql <- sprintf("
      WITH streams AS (%s),
      us AS (   -- sum a song's listening time across files before weighting
        SELECT s.hashed_id, s.period, s.song_id, sum(s.lt) AS l
        FROM streams s
        SEMI JOIN users_rel u ON u.hashed_id = s.hashed_id
        GROUP BY 1,2,3
      ),
      joined AS (
        SELECT us.hashed_id, us.period, us.l, %s
        FROM us JOIN feat_rel f USING (song_id)
      ),
      counted AS (
        SELECT *, count(*) OVER (PARTITION BY hashed_id, period) AS n_all
        FROM joined
      ),
      suff AS (
        SELECT hashed_id, period, any_value(n_all) AS n_all, count(*) AS n, sum(l) AS W,
               %s
        FROM counted WHERE l > 0
        GROUP BY 1,2
        HAVING any_value(n_all) > 1
      )
      SELECT hashed_id, period,
             %s
      FROM suff",
      duck_stream_cte(streaming_files, since_ts, interval, bucket),
      feat_cols, suff, finals)
    DBI::dbGetQuery(con, sql)
  }, .threads = threads, .memory_limit = memory_limit)
}
