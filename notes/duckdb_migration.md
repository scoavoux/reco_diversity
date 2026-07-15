# DuckDB migration — streams hot path (proof of concept)

This is the first slice of the larger refactor: move data IO + heavy aggregation
onto DuckDB, keep statistics in R. It converts the **streams hot path** — the
biggest bottleneck — and establishes the reusable foundation the rest of the
imports will migrate onto.

## What changed

| Before | After |
| --- | --- |
| `make_user_song_per_period_onefile()` mapped per file (sequential), then `merge_user_song_per_period()` `bind_rows`-ed **every** branch into one giant `user_song_per_period` target on S3 | Gone. Nothing materialises user × song × week in R. |
| `make_user_artist_per_period(user_song_per_period, …)` | `make_user_artist_per_period_duck(streaming_data_files, users, items, artists_to_remove)` — reads the stream parquet directly, joins song→artist and aggregates **inside DuckDB**, returns only user × artist × week. |
| `compute_acoustic_diversity(user_song_per_period, …)` | `compute_acoustic_diversity_duck(streaming_data_files, acoustic_features_with_pca, users)` — computes the weighted moments straight from streams via **sufficient statistics**, never holding the song-level table. |

New files:
- `R/duckdb_functions.R` — the foundation: `duck_connect()`, `with_duck()`,
  `duck_attach_s3()`, and generic `duck_read_parquet()` / `duck_read_csv()`
  readers (templates for migrating the remaining `make_*` imports).
- `R/make_data_duck.R` — the streams-path rewrite.

Old functions in `R/make_data.R` are left in place, and the old targets are kept
commented in `_targets.R`, so rollback is uncommenting three lines.

## The boundary (2a)

**DuckDB does IO + joins + group-bys; R does the statistics.** The only stat
pushed into SQL is the *finalisation* of the acoustic weighted mean/SD, because
it is exact and trivial (`mean = Sx/W`, `sd = sqrt(n/(n-1)·(Sxx/W − mean²))`).
Everything modelling-side — `fixest`, `irlba` PCA, `igraph` infomap — stays in R.

## Why it's faster / lighter

- The giant `user_song_per_period` object is never built, stored on S3, or
  reloaded by the ~10 downstream `compute_*` targets.
- Acoustic diversity uses **additive sufficient statistics** (`W=Σl`, `Sx=Σl·x`,
  `Sxx=Σl·x²`, `n`) instead of a huge weighted `group_by`. Only
  `n_users × n_weeks × n_features` rows come back to R.
- The per-file sequential map is replaced by one query; DuckDB multithreads the
  scan and spills to disk instead of OOM-ing. Tune with the `threads` /
  `memory_limit` args (they map to `PRAGMA threads` / `memory_limit`).

## Correctness — verified

The SQL was checked to reproduce the previous tidytable results **to floating
point** (max Δ ≈ 3e-13 on moments, 0 on `l_play`/`n_play`) with harnesses in the
project scratchpad (`verify_wtd.py`, `verify_streams.py`, `verify_r_sql.py`).
The DuckDB engine is identical across the Python and R bindings, so a query that
passes there is the query used in R. Coverage:

- songs split across **long/short** files summed **before** weighting;
- `truncate_hashed_id()` → `regexp_extract(hashed_id,'^\w{9}')`, including the
  id-collision merging that truncation causes;
- `breakdown_time(ts,'week')` → `strftime(to_timestamp(ts_listen),'%Y-%W')`
  (corroborated by the existing `trim_first_week` filter dropping `-00`, exactly
  the partial first week `%W` emits);
- `is_listened`/`media_type`/`ts_listen` filters and `listening_time<0 → 0`;
- `Hmisc::wtd.mean` / `wtd.var(normwt = TRUE)` for every acoustic feature + PC.

One faithfully-reproduced edge: when a user-week has `n_all>1` featured songs but
only one with `l>0`, both the old code and this SQL produce a degenerate SD
(`wtd.var` over one value). It is preserved, not fixed — flagging it as a latent
issue in the original definition to decide on separately.

## Configuration

`duck_connect()` reads the same environment variables as `initialize_s3()`:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_S3_ENDPOINT` (bare host), `AWS_DEFAULT_REGION`. It sets `URL_STYLE 'path'`
and `USE_SSL true` for the non-AWS (SSP Cloud / MinIO) endpoint. A fresh secret
is created per connection — correct for the weekly-expiring session token, since
every Monday run re-reads the current token.

Spill directory defaults to `data/temp/duck_spill` (created on demand).

## Weekly-refresh gotcha (build this in)

`targets` invalidates on R code + upstream target *values*, **not** on the state
of S3 objects. `streaming_data_files` (the key list) changes when **new** files
appear, so downstream rebuilds. But if existing keys are **overwritten in
place**, the list is unchanged and `targets` will serve stale cached results.

To make the Monday refresh reliable, fold each object's **ETag/last-modified**
into the `streaming_data_files` target so a content change changes the value:

```r
list_streaming_data_files <- function(){
  s3 <- initialize_s3()
  obj <- s3$list_objects_v2(Bucket = "scoavoux", Prefix = "records_w3/streams")$Contents
  tibble::tibble(
    key  = vapply(obj, `[[`, "", "Key"),
    etag = vapply(obj, `[[`, "", "ETag")
  ) |> dplyr::filter(!stringr::str_detect(key, "\\.keep"))
}
```

Pass `.$key` to the DuckDB functions; the `etag` column rides along so any
in-place change re-triggers the hot path. (Alternatively `tarchetypes::tar_age()`
or a scheduled `tar_invalidate()`.)

## Next

1. Migrate the remaining `make_*` S3 reads onto `duck_read_parquet` /
   `duck_read_csv` (mechanical; `make_items_data`, `make_genre_data`, the
   musicbrainz joins).
2. Generic Hill-number diversity engine `compute_div(f, q)` +
   `compute_categorical_diversity()`.
3. Spec-grid analysis architecture (retire the YAML + per-outcome loop).
4. Track-level language / genre / popularity through the generic engine.
5. GfK exogenous popularity (ISRC → track_id crosswalk).
