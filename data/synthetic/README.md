# Synthetic data set for `reco_diversity`

> **SYNTHETIC, NON-IDENTIFYING DATA — NOT REAL DATA.**
>
> This directory is the target for the synthetic copy of the analysis inputs.
> It is **empty in the repository**; the data files are produced locally by
> running `make_synthetic_data.R` (which needs S3 access, run once by the
> author) and are git-ignored. When you run the generator it **overwrites this
> file** with a live manifest listing every file it wrote, the seed, and the
> fidelity mode.

## Purpose

Let the whole analysis pipeline run **end-to-end offline**, without S3, so the
code can be developed and tested on a laptop (including by an AI assistant), and
to seed a computational-reproducibility replication package. The synthetic data
only needs to make the code **run and produce output of the correct shape** — it
does **not** reproduce the real estimates.

## How it is built (and why it's safe)

`make_synthetic_data.R` reads the **real** inputs only to learn each column's
empirical **margin** and categorical **levels**, then writes a small data set in
which:

- **All keys are fresh surrogates** — user ids (`u00000001`, …), synthetic
  artist/song ids, `synthmbid-…`. No real `hashed_id`, artist id, song id or
  musicbrainz id is reused.
- **Value columns are independent resamples of real marginal distributions
  only** — no real individual's multivariate record survives.
- **Referential integrity is preserved** — keys that join tables (user, artist,
  song, mbid, area, `period`) stay mutually consistent across files.
- **The panel grid is intact** — observation level is user × week; each user
  appears across many periods, including a 2019 window for the Bartik baseline,
  with many users per period for leave-one-out platform means.

## Running the pipeline against it

A single environment variable switches the whole pipeline to the local
synthetic data (see `R/common_functions.R::use_synthetic_data`):

```sh
RECO_DIVERSITY_DATA=synthetic Rscript -e 'targets::tar_make()'
```

In that mode `initialize_s3()` returns a local-filesystem mock that reads this
directory (mirroring the real S3 key layout), and `_targets.R` stores
intermediate targets locally instead of on S3. With the variable unset (or
`real`) the pipeline behaves exactly as before, reading the real S3 data.

## Generating

```sh
# default: "panel" mode (within-user structure + a positive recommendation/
# diversity link; estimates are plausibly signed but ARBITRARY in magnitude)
Rscript make_synthetic_data.R

# or the most conservative de-identification (estimates ~null/noise):
RECO_DIVERSITY_SYNTHETIC_REALISM=structural Rscript make_synthetic_data.R
```

`set.seed(20240613)` makes generation reproducible.

## Controlling the size (keep it small)

All sizes are set by the `cfg` list at the top of `make_synthetic_data.R`, and
every knob is also an environment variable so you can shrink the set without
editing the file. The stream table is the only large one; its row count is
roughly `n_users × (weeks_2019 + weeks_post) × activity × plays_lambda`, so
those four knobs are the levers that matter.

```sh
# even smaller than the default ~300 users / ~330k rows:
RECO_DIVERSITY_SYN_N_USERS=150 \
RECO_DIVERSITY_SYN_PLAYS=25 \
RECO_DIVERSITY_SYN_WEEKS_POST=4 \
  Rscript make_synthetic_data.R
```

| Env var | Default | Controls |
|---|---|---|
| `RECO_DIVERSITY_SYN_N_USERS`     | 300  | number of users (rows scale linearly) |
| `RECO_DIVERSITY_SYN_PLAYS`       | 40   | mean plays per active user-period (rows scale linearly) |
| `RECO_DIVERSITY_SYN_WEEKS_2019`  | 22   | 2019 weekly periods (Bartik baseline window) |
| `RECO_DIVERSITY_SYN_WEEKS_POST`  | 6    | post-2019 weekly periods |
| `RECO_DIVERSITY_SYN_N_ARTISTS`   | 2000 | distinct artists |
| `RECO_DIVERSITY_SYN_N_SONGS`     | 5000 | distinct songs |
| `RECO_DIVERSITY_SYN_ACTIVITY`    | 0.98 | P(user active in a period) |
| `RECO_DIVERSITY_SYN_REPERTOIRE`  | 50   | distinct artists per user |

The defaults are deliberately small. They work because, **in synthetic mode
only**, the pipeline's large-data thresholds auto-relax (real runs are
unchanged): `make_user_period_level_data` drops the 1000-users-per-period filter
to 50 and the 2-hours filter to 0.5, and `make_recoshare_instrument` drops the
Bartik baseline requirement from `>20 weeks / >100 h` in 2019 to `>=8 weeks /
>=10 h`. These too are overridable — `RECO_DIVERSITY_MIN_USERS_PER_PERIOD`,
`RECO_DIVERSITY_MIN_HOURS`, `RECO_DIVERSITY_BASELINE_MIN_WEEKS`,
`RECO_DIVERSITY_BASELINE_MIN_HOURS`. If you shrink `n_users` below the
per-period threshold, or `weeks_2019` below the baseline week threshold, the
pipeline will filter the data down to nothing, so keep them above.

## File layout (mirrors the real S3 bucket `scoavoux/`)

| Key (relative to this directory)                          | Format  | Key columns |
|-----------------------------------------------------------|---------|-------------|
| `records_w3/RECORDS_hashed_user_group.parquet`            | parquet | `hashed_id` |
| `records_w3/streams/streams_short/REGION=*/part-0.parquet`| parquet | `hashed_id`, `media_id`, `context_4`, `ts_listen` |
| `records_w3/items/song.snappy.parquet`                    | parquet | `song_id` → `artist_id` |
| `records_w3/items/songs.snappy.parquet`                   | parquet | `song_id` → `artist_id` |
| `records_w3/items/artist.snappy.parquet`                  | parquet | `artist_id`, `name`, `main_genre` |
| `records_w3/items/artists_data.snappy.parquet`            | parquet | `artist_id`, `name`, `main_genre` |
| `records_w3/250205-deezer-with-audio-ft.feather`          | feather | `song_id` + 9 acoustic features |
| `records_w3/artists_pop.csv`                              | csv     | `artist_id`, `nb_fans` |
| `records_w3/artists_songs_languages.csv`                  | csv     | `art_id`, `lang` |
| `records_w3/related_artists/related_artists.csv`          | csv     | `orig_artist_id`, `dest_artist_id` |
| `musicbrainz/mbid_deezerid.csv`                           | csv     | `mbid` ↔ `artist_id` |
| `musicbrainz/mbz_gender.csv`                              | csv     | `gid` (mbid), `gender` |
| `musicbrainz/mbid_area.csv`                               | csv     | `gid` (mbid), `area` |
| `musicbrainz/area_names.csv`                              | csv     | `id`, `name`, `type` |
| `musicbrainz/mbid_release_group.csv`                      | csv     | `mbid`, `first_release_date_year` |
| `omnivorism/objects/artists_csv`                          | csv     | `artist_id`, `sc_*` |
| `omnivorism/objects/users_csv`                            | csv     | `hashed_id` (supplementary survey only) |

See `make_synthetic_data.R` for the authoritative schema and the per-column
resampling logic.
