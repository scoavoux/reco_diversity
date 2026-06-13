#!/usr/bin/env Rscript
# =============================================================================
# make_synthetic_data.R
# -----------------------------------------------------------------------------
# Generate a small, non-identifying, *synthetic* copy of every input file the
# reco_diversity analysis pipeline reads from S3, so the pipeline can run
# end-to-end offline (e.g. on a laptop, or by an AI assistant) and to seed a
# computational-reproducibility replication package.
#
# WHO RUNS THIS, AND WHEN
#   The author, ONCE, on a machine that has S3 access (the same machine that
#   runs the real pipeline). It reads the *real* inputs only to learn each
#   column's empirical margin and categorical levels, then writes a synthetic
#   data set built from SURROGATE keys and resampled values.
#
# WHAT IT GUARANTEES
#   * Structural fidelity: same set of files, paths, formats, column names and
#     types, categorical levels (genres, languages, countries, gender codes)
#     and value domains (shares, "YYYY-WW" periods, ...) as the real data.
#   * De-identification: keys are fresh surrogates ("u00000001", synthetic
#     artist/song ids, "synthmbid-..."); no real hashed_id, artist or song id
#     is reused, and no real individual's multivariate row survives. Value
#     columns are independent resamples of real *marginal* distributions only.
#   * Referential integrity: keys that join tables (user id, artist id, song
#     id, mbid, area id, period) are mutually consistent across all files.
#   * Panel grid: observation level is user x week; each user appears across
#     many periods, including a 2019 window for the Bartik baseline, with
#     >1 user per period for leave-one-out platform means.
#
# OUTPUT
#   Files under data/synthetic/<same relative S3 key>, e.g.
#     data/synthetic/records_w3/items/song.snappy.parquet
#   plus data/synthetic/README.md (manifest). Then run the pipeline offline:
#     RECO_DIVERSITY_DATA=synthetic Rscript -e 'targets::tar_make()'
#
# TWO FIDELITY MODES (env var RECO_DIVERSITY_SYNTHETIC_REALISM)
#   "panel"      (default) -- each user gets a persistent listening repertoire
#                  and recommendation propensity, so within-user serial
#                  structure and a (positive) recommendation<->diversity link
#                  exist and FE/IV estimates are non-null and plausibly signed.
#                  NOTE: magnitudes are arbitrary; this does NOT reproduce the
#                  real estimates. It is for "the code produces sensible-looking
#                  output", not for demonstrating findings.
#   "structural" -- value columns are resampled independently within the
#                  (user, period) grid. Strongest de-identification; FE/IV
#                  estimates are ~null/noise. Use when you only need the code
#                  to run and want the most conservative privacy guarantee.
#
# IMPORTANT: This script reads real data; do NOT commit anything it reads. The
# synthetic outputs are safe but are git-ignored by default (see .gitignore);
# commit them deliberately if you want them in the replication package.
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(bit64)
})

# Pull in initialize_s3_real(), synthetic_data_dir(), etc.
source(here::here("R", "common_functions.R"))

# ---- Configuration ----------------------------------------------------------
set.seed(20240613)

cfg <- list(
  realism        = tolower(Sys.getenv("RECO_DIVERSITY_SYNTHETIC_REALISM", "panel")),
  n_users        = 1150L,   # > min_users_per_period (1000) so periods survive
  n_artists      = 3000L,
  n_songs        = 8000L,
  n_clusters     = 20L,     # related-artist communities (each >=10 -> kept)
  repertoire     = 60L,     # distinct artists a given user ever listens to
  mbid_coverage  = 0.85,    # share of artists with a musicbrainz id
  weeks_2019     = 24L,     # >= 20 -> Bartik baseline eligibility
  weeks_post     = 16L,     # post-2019 periods -> instrument is defined
  activity       = 0.98,    # P(user active in a given period)
  plays_lambda   = 90,      # mean plays per active user-period
  lt_mean        = 220,     # mean listening_time per play (seconds)
  region_levels  = c("FR", "OT")  # streams partitions -> dynamic branches
)
stopifnot(cfg$realism %in% c("panel", "structural"))
message("Synthetic data generator -- realism mode: ", cfg$realism)

out_root <- synthetic_data_dir()
dir.create(out_root, recursive = TRUE, showWarnings = FALSE)

# ---- IO helpers -------------------------------------------------------------

# Write `obj` to data/synthetic/<key> in the format implied by the key.
write_synth <- function(obj, key) {
  path <- file.path(out_root, key)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (grepl("\\.parquet$", key)) {
    arrow::write_parquet(obj, path, compression = "snappy")
  } else if (grepl("\\.feather$", key)) {
    arrow::write_feather(obj, path)
  } else if (grepl("\\.csv$", key)) {
    readr::write_csv(obj, path)
  } else {
    # Keys with no extension (omnivorism/objects/*_csv) are CSV in this project.
    readr::write_csv(obj, path)
  }
  message("  wrote ", key, "  (", nrow(obj), " rows)")
  invisible(path)
}

# Read a real input column-margin, tolerating a missing/unreadable source by
# falling back to a synthetic default. Returns a vector to resample from.
real_margin <- function(reader, fallback) {
  out <- tryCatch(reader(), error = function(e) {
    message("  [margin] could not read real source (", conditionMessage(e),
            ") -- using synthetic fallback")
    NULL
  })
  if (is.null(out) || length(out) == 0) fallback else out
}

# Sample n values from an empirical margin (drops NA). Index-based to avoid R's
# sample()-with-a-length-1-numeric trap.
margin_sample <- function(real, n) {
  real <- real[!is.na(real)]
  if (length(real) == 0) stop("empty margin passed to margin_sample()")
  real[sample.int(length(real), n, replace = TRUE)]
}

s3 <- tryCatch(initialize_s3_real(),
               error = function(e) { message("No S3 client: ", conditionMessage(e)); NULL })

read_s3 <- function(key, how) {
  if (is.null(s3)) stop("S3 unavailable")
  body <- s3$get_object(Bucket = "scoavoux", Key = key)$Body
  how(body)
}

# =============================================================================
# 1. LEARN MARGINS / LEVELS FROM THE REAL DATA (values only -- never keys/ids)
# =============================================================================
message("Reading real margins from S3 ...")

# Categorical genre levels are authoritative in the repo (codes.csv), so we use
# the recode scheme's source levels directly -- no S3 needed.
genre_levels <- {
  codes <- readr::read_csv(here::here("data", "codes.csv"), col_types = "ccc")
  unique(codes$orig[codes$scheme == "deezer_maingenre"])
}

# main_genre frequency margin (from a real artist file) -- else uniform levels.
main_genre_pool <- real_margin(
  function() read_s3("records_w3/items/artist.snappy.parquet",
                     function(b) read_parquet(b, col_select = "main_genre")$main_genre),
  fallback = genre_levels
)
main_genre_pool <- main_genre_pool[main_genre_pool %in% genre_levels]
if (length(main_genre_pool) == 0) main_genre_pool <- genre_levels

# nb_fans margin
nb_fans_pool <- real_margin(
  function() read_s3("records_w3/artists_pop.csv",
                     function(b) read_csv(b, col_select = "nb_fans", show_col_types = FALSE)$nb_fans),
  fallback = round(rlnorm(5000, log(500), 1.6))
)

# gender code margin (kept codes are 1 = men, 2 = women)
gender_pool <- real_margin(
  function() read_s3("musicbrainz/mbz_gender.csv",
                     function(b) read_csv(b, show_col_types = FALSE)$gender),
  fallback = sample(c(1L, 2L), 5000, replace = TRUE, prob = c(.7, .3))
)
gender_pool <- gender_pool[gender_pool %in% c(1, 2)]
if (length(gender_pool) == 0) gender_pool <- c(1L, 2L)

# country name levels (area_names type == 1)
country_pool <- real_margin(
  function() read_s3("musicbrainz/area_names.csv", function(b) {
    a <- read_csv(b, show_col_types = FALSE)
    a$name[a$type == 1]
  }),
  fallback = c("France", "United States", "United Kingdom", "Germany", "Italy",
               "Spain", "Belgium", "Canada", "Brazil", "Japan", "Sweden",
               "Netherlands", "Mexico", "Argentina", "Portugal", "Australia",
               "Nigeria", "South Korea", "Colombia", "Ireland")
)

# language margin (fr/en must exist; downstream only uses fr & en explicitly)
lang_pool <- real_margin(
  function() read_s3("records_w3/artists_songs_languages.csv",
                     function(b) read_csv(b, show_col_types = FALSE)$lang),
  fallback = sample(c("fr", "en", "es", "de", "it", "pt"), 5000, replace = TRUE,
                    prob = c(.35, .35, .1, .08, .07, .05))
)
lang_pool <- lang_pool[!is.na(lang_pool)]
if (!any(c("fr", "en") %in% lang_pool)) lang_pool <- c(lang_pool, "fr", "en")

# first release year margin
release_year_pool <- real_margin(
  function() read_s3("musicbrainz/mbid_release_group.csv",
                     function(b) read_csv(b, show_col_types = FALSE)$first_release_date_year),
  fallback = sample(1960:2024, 5000, replace = TRUE)
)
release_year_pool <- as.integer(release_year_pool[!is.na(release_year_pool)])
release_year_pool <- release_year_pool[release_year_pool >= 1900 & release_year_pool <= 2025]
if (length(release_year_pool) == 0) release_year_pool <- 1960:2024

# omnivorism sc_ score margins
sc_cols <- c("sc_endo_isei", "sc_endo_educ", "sc_exo_press", "sc_exo_score", "sc_exo_radio")
sc_pools <- real_margin(
  function() {
    d <- read_s3("omnivorism/objects/artists_csv",
                 function(b) read_csv(b, show_col_types = FALSE))
    d <- dplyr::select(d, dplyr::any_of(sc_cols))
    lapply(sc_cols, function(cc) if (cc %in% names(d)) d[[cc]] else NULL)
  },
  fallback = NULL
)
if (is.null(sc_pools)) {
  sc_pools <- list(rnorm(5000, 45, 12), rnorm(5000, .4, .12),
                   rlnorm(5000, 1, 1), rnorm(5000, 6, 1.5), rlnorm(5000, 2, 1))
}
names(sc_pools) <- sc_cols
sc_pools <- lapply(sc_pools, function(v) {
  v <- v[!is.na(v)]; if (length(v) == 0) rnorm(2000) else v
})

# acoustic feature margins
acoustic_cols <- c("danceability", "energy", "loudness", "speechiness",
                   "acousticness", "instrumentalness", "liveness", "valence", "tempo")
acoustic_pools <- real_margin(
  function() {
    d <- read_s3("records_w3/250205-deezer-with-audio-ft.feather",
                 function(b) read_feather(b, col_select = acoustic_cols))
    d <- dplyr::slice_sample(d, n = min(nrow(d), 50000))
    lapply(acoustic_cols, function(cc) d[[cc]])
  },
  fallback = NULL
)
if (is.null(acoustic_pools)) {
  acoustic_pools <- list(
    runif(5000, 0, 1), runif(5000, 0, 1), runif(5000, -30, 0), rbeta(5000, 1.5, 8),
    rbeta(5000, 1.5, 4), rbeta(5000, 1.2, 6), rbeta(5000, 1.5, 6), runif(5000, 0, 1),
    rnorm(5000, 120, 30))
}
names(acoustic_pools) <- acoustic_cols
acoustic_pools <- lapply(acoustic_pools, function(v) { v <- v[!is.na(v)]; if (!length(v)) rnorm(2000) else v })

# listening_time & context_4 margins (read one real streams part only)
stream_margin <- real_margin(
  function() {
    keys <- s3$list_objects_v2(Bucket = "scoavoux", Prefix = "records_w3/streams")$Content %>%
      map_chr(~ .x$Key)
    keys <- keys[!str_detect(keys, "\\.keep")]
    key <- keys[str_detect(keys, "short")][1]
    if (is.na(key)) key <- keys[1]
    d <- read_s3(key, function(b) read_parquet(b, col_select = c("listening_time", "context_4")))
    dplyr::slice_sample(d, n = min(nrow(d), 200000))
  },
  fallback = NULL
)
if (is.null(stream_margin)) {
  lt_pool <- pmax(0, round(rgamma(5000, shape = 4, scale = cfg$lt_mean / 4)))
  context_levels <- c("organic", "reco_algo", "edito", "ext", "other")
} else {
  lt_pool <- stream_margin$listening_time
  lt_pool <- pmax(0, lt_pool[!is.na(lt_pool)])
  context_levels <- unique(na.omit(stream_margin$context_4))
}
# Guarantee the contexts the analysis depends on are present.
for (need in c("organic", "reco_algo", "edito"))
  if (!need %in% context_levels) context_levels <- c(context_levels, need)

# =============================================================================
# 2. SURROGATE KEYS (fresh ids -- the core de-identification layer)
# =============================================================================
message("Building surrogate keys ...")

# Users: 9-char word-character ids (truncate_hashed_id keeps first 9 unchanged).
user_ids <- sprintf("u%08d", seq_len(cfg$n_users))

# Artist / song ids as int64 (matches real catalog id type), offset to make
# clear they are synthetic and to avoid any resemblance to real ids.
artist_ids <- as.integer64(900000000L) + seq_len(cfg$n_artists)
song_ids   <- as.integer64(800000000L) + seq_len(cfg$n_songs)

# Each song belongs to exactly one artist (referential integrity for items).
song_artist <- tibble(
  song_id   = song_ids,
  artist_id = sample(artist_ids, cfg$n_songs, replace = TRUE)
)
artist_songs <- split(as.character(song_artist$song_id),
                      as.character(song_artist$artist_id))

# One representative song per artist (artists with no song get a random one).
# Lets the stream generator map plays -> songs vectorially; each user-period
# still has >= 2 distinct songs (it has >= 2 distinct artists), which is what
# the acoustic-diversity computation needs.
artist_primary_song <- vapply(as.character(artist_ids), function(a) {
  sg <- artist_songs[[a]]
  if (is.null(sg)) NA_character_ else sg[1]
}, character(1), USE.NAMES = FALSE)
na_song <- is.na(artist_primary_song)
artist_primary_song[na_song] <- sample(as.character(song_ids), sum(na_song), replace = TRUE)

# musicbrainz coverage: a subset of artists carry an mbid.
has_mbid    <- sample(c(TRUE, FALSE), cfg$n_artists, replace = TRUE,
                      prob = c(cfg$mbid_coverage, 1 - cfg$mbid_coverage))
mbid_artists <- artist_ids[has_mbid]
mbids        <- sprintf("synthmbid-%06d", seq_along(mbid_artists))

# Related-artist communities (>= 10 members each so infomap keeps them).
artist_cluster <- sample(seq_len(cfg$n_clusters), cfg$n_artists, replace = TRUE)

# =============================================================================
# 3. REFERENCE / ENRICHMENT TABLES (artist- and song-level)
# =============================================================================
message("Writing reference tables ...")

# 3a. items: song_id -> artist_id, split across the "old" and "new" files.
# Both are bound and de-duplicated on song_id by make_items_data().
new_idx <- sample(c(TRUE, FALSE), cfg$n_songs, replace = TRUE, prob = c(.8, .2))
write_synth(song_artist[new_idx, ],  "records_w3/items/song.snappy.parquet")
write_synth(song_artist[!new_idx, ], "records_w3/items/songs.snappy.parquet")

# 3b. artist tables: columns MUST be ordered artist_id, name, main_genre, ...
# (make_genre_data reads cols 1:3, make_unique_artists reads cols 1:2).
artist_tbl <- tibble(
  artist_id  = artist_ids,
  name       = sprintf("Synthetic Artist %05d", seq_len(cfg$n_artists)),
  main_genre = sample(main_genre_pool, cfg$n_artists, replace = TRUE),
  tag_labels = NA_character_
)
# "new" file gets all artists; "old" file a subset (bind + distinct downstream).
write_synth(artist_tbl, "records_w3/items/artist.snappy.parquet")
write_synth(dplyr::slice_sample(artist_tbl, prop = .5),
            "records_w3/items/artists_data.snappy.parquet")

# 3c. artist popularity
write_synth(
  tibble(artist_id = artist_ids,
         nb_fans   = as.integer(round(margin_sample(nb_fans_pool, cfg$n_artists)))),
  "records_w3/artists_pop.csv"
)

# 3d. acoustic features (feather), one row per song; ALL songs covered so any
# user-period with >= 2 songs yields an acoustic-diversity row.
acoustic_tbl <- tibble(song_id = as.integer64(song_ids))
for (cc in acoustic_cols)
  acoustic_tbl[[cc]] <- margin_sample(acoustic_pools[[cc]], cfg$n_songs)
write_synth(acoustic_tbl, "records_w3/250205-deezer-with-audio-ft.feather")

# 3e. languages: one row per artist (art_id), single language each.
write_synth(
  tibble(art_id   = artist_ids,
         lang     = sample(lang_pool, cfg$n_artists, replace = TRUE),
         nb_songs = as.integer(sample(1:50, cfg$n_artists, replace = TRUE))),
  "records_w3/artists_songs_languages.csv"
)

# 3f. musicbrainz id bridge + gender + area + release year (keyed by mbid).
write_synth(
  tibble(mbid = mbids, artist_id = mbid_artists),
  "musicbrainz/mbid_deezerid.csv"
)
write_synth(
  tibble(gid    = mbids,
         gender = as.integer(sample(gender_pool, length(mbids), replace = TRUE))),
  "musicbrainz/mbz_gender.csv"
)

# Countries: a fixed level set (type == 1), then assign each mbid a country.
countries <- tibble(
  id   = seq_along(unique(country_pool)),
  name = unique(country_pool),
  type = 1L
)
# Add a couple of non-country areas to exercise the `type == 1` filter.
countries <- bind_rows(countries,
                       tibble(id = max(countries$id) + 1:2,
                              name = c("Europe", "Synthetic City"), type = c(2L, 3L)))
write_synth(countries, "musicbrainz/area_names.csv")
write_synth(
  tibble(gid  = mbids,
         area = sample(countries$id[countries$type == 1], length(mbids), replace = TRUE)),
  "musicbrainz/mbid_area.csv"
)
write_synth(
  tibble(mbid                    = mbids,
         first_release_date_year = as.integer(margin_sample(release_year_pool, length(mbids)))),
  "musicbrainz/mbid_release_group.csv"
)

# 3g. related-artists edge list. Dense intra-community + sparse inter-community
# edges so cluster_infomap recovers communities of size >= 10.
# ids kept as character here (avoids integer64 bind issues); written to CSV as
# integer strings exactly like the real edge list.
rel_edges <- map_dfr(seq_len(cfg$n_clusters), function(k) {
  members <- as.character(artist_ids[artist_cluster == k])
  if (length(members) < 2) return(NULL)
  # each member points to a few other members of its community
  tibble(
    orig_artist_id = rep(members, each = 4),
    dest_artist_id = vapply(rep(members, each = 4),
                            function(m) sample(setdiff(members, m), 1),
                            character(1))
  )
})
# a handful of cross-community edges (noise) -- still leaves communities intact
n_cross <- round(nrow(rel_edges) * 0.02)
rel_edges <- bind_rows(rel_edges, tibble(
  orig_artist_id = as.character(sample(artist_ids, n_cross, replace = TRUE)),
  dest_artist_id = as.character(sample(artist_ids, n_cross, replace = TRUE))
))
rel_edges <- rel_edges %>%
  filter(orig_artist_id != dest_artist_id) %>%
  mutate(name     = "Synthetic Related Artist",
         nb_album = as.integer(sample(1:30, n(), replace = TRUE)),
         nb_fan   = as.integer(round(margin_sample(nb_fans_pool, n()))))
write_synth(rel_edges, "records_w3/related_artists/related_artists.csv")

# 3h. omnivorism artist scores (sc_*)
omni <- tibble(artist_id = artist_ids)
for (cc in sc_cols) omni[[cc]] <- margin_sample(sc_pools[[cc]], cfg$n_artists)
write_synth(omni, "omnivorism/objects/artists_csv")

# =============================================================================
# 4. USERS + PANEL GRID + STREAMS  (the user x week observation level)
# =============================================================================
message("Building user table and panel grid ...")

# 4a. user group file (control group, paying). is_respondent toggled for the
# supplementary survey analysis. Real loader filters is_in_control_group &
# pay_offer then truncates hashed_id (our ids are already 9 chars).
write_synth(
  tibble(hashed_id           = user_ids,
         is_in_control_group = TRUE,
         pay_offer           = TRUE,
         is_respondent       = sample(c(TRUE, FALSE), cfg$n_users, replace = TRUE, prob = c(.3, .7))),
  "records_w3/RECORDS_hashed_user_group.parquet"
)

# optional survey file (used only by the supplementary context_by_social_status)
write_synth(
  tibble(hashed_id = user_ids,
         age       = as.integer(sample(16:75, cfg$n_users, replace = TRUE)),
         gender    = sample(c("Man", "Woman"), cfg$n_users, replace = TRUE),
         degree    = sample(c("None", "Secondary", "Higher"), cfg$n_users, replace = TRUE)),
  "omnivorism/objects/users_csv"
)

# 4b. Period calendar. Use real Mondays so format.Date(ts, "%Y-%W") reproduces
# our chosen period labels exactly (period is derived from ts downstream).
all_mondays <- seq(as.Date("2019-01-07"), as.Date("2024-12-23"), by = "week")
md_year     <- as.integer(format(all_mondays, "%Y"))
mondays_2019 <- all_mondays[md_year == 2019][seq_len(cfg$weeks_2019)]
mondays_post <- all_mondays[md_year > 2019]
mondays_post <- mondays_post[round(seq(1, length(mondays_post), length.out = cfg$weeks_post))]
period_mondays <- sort(c(mondays_2019, mondays_post))
period_labels  <- format(period_mondays, "%Y-%W")
period_tbl <- tibble(monday = period_mondays, period = period_labels) %>%
  distinct(period, .keep_all = TRUE)
n_periods <- nrow(period_tbl)
message("  ", n_periods, " periods (", sum(md_year[match(period_tbl$monday, all_mondays)] == 2019),
        " in 2019); ", cfg$n_users, " users")

# 4c. Per-user latent traits + a personal repertoire of artists.
user_tbl <- tibble(
  hashed_id      = user_ids,
  reco_propensity = rnorm(cfg$n_users),   # tendency to use recommendation
  breadth        = rnorm(cfg$n_users)     # tendency to listen broadly
)
user_repertoire <- lapply(seq_len(cfg$n_users), function(i)
  sample(seq_len(cfg$n_artists), cfg$repertoire))

plogis <- function(x) 1 / (1 + exp(-x))

# 4d. Generate streams cell by cell (active user-periods). Each cell emits
# play-level rows: hashed_id, ts_listen, media_id (song), is_listened,
# listening_time, media_type, context_4.
message("Generating streams (", cfg$realism, " mode) ...")

period_trend <- seq(-0.3, 0.6, length.out = n_periods)  # reco rises over time
cells <- vector("list", cfg$n_users * n_periods)
ci <- 0L

for (ui in seq_len(cfg$n_users)) {
  active <- runif(n_periods) < cfg$activity
  rep_artists <- user_repertoire[[ui]]
  theta <- user_tbl$reco_propensity[ui]
  delta <- user_tbl$breadth[ui]
  for (ti in which(active)) {
    n_play <- max(20L, rpois(1, cfg$plays_lambda))

    if (cfg$realism == "panel") {
      # persistent, correlated structure
      reco_share <- plogis(-0.2 + 0.9 * theta + period_trend[ti] + rnorm(1, 0, 0.4))
      k_art <- max(2L, round(8 + 6 * delta + 8 * reco_share + rnorm(1, 0, 2)))
      # draw distinct artists mostly from the user's repertoire (within-user
      # persistence); broader users + more reco -> wider draws.
      pool <- rep_artists
      chosen <- sample(pool, min(k_art, length(pool)))
      # editorial vs algorithmic split tilts with the user's propensity
      edito_frac <- plogis(0.2 * theta)
    } else {
      # structural: independent, no user persistence
      reco_share <- plogis(rnorm(1, 0, 0.8))
      k_art <- max(2L, round(runif(1, 4, 28)))
      chosen <- sample(seq_len(cfg$n_artists), k_art)
      edito_frac <- 0.5
    }

    # distribute plays across chosen artists (Zipf-ish weights)
    w <- 1 / seq_along(chosen)^0.8
    art_of_play <- sample(chosen, n_play, replace = TRUE, prob = w)
    # map each play to its artist's representative song (vectorised)
    s_of_play <- artist_primary_song[art_of_play]

    # context per play: organic vs recommendation (split edito/algo), plus a
    # little ext/other noise that the pipeline filters out.
    u <- runif(n_play)
    ctx <- ifelse(
      u < 0.03, sample(c("ext", "other"), n_play, replace = TRUE),
      ifelse(u < 0.03 + reco_share * 0.97,
             ifelse(runif(n_play) < edito_frac, "edito", "reco_algo"),
             "organic"))

    ts0 <- as.integer(as.POSIXct(period_tbl$monday[ti], tz = "UTC"))
    ci <- ci + 1L
    cells[[ci]] <- tibble(
      hashed_id     = user_ids[ui],
      ts_listen     = ts0 + sample.int(6L * 86400L, n_play, replace = TRUE),
      media_id      = s_of_play,            # character; -> int64 after bind
      is_listened   = 1L,
      listening_time = margin_sample(lt_pool, n_play),
      media_type    = "song",
      context_4     = ctx
    )
  }
}
cells <- cells[seq_len(ci)]
streams <- bind_rows(cells)                 # base-typed columns only (safe bind)
streams$media_id <- as.integer64(streams$media_id)  # restore int64 key type
message("  ", nrow(streams), " stream rows across ", ci, " user-periods")

# 4e. Write streams as a partitioned dataset under streams_short/REGION=*/ .
# Keys contain "short" (-> short schema branch) and the partitioning also lets
# the supplementary rythms plot open_dataset() it directly.
streams$REGION <- sample(cfg$region_levels, nrow(streams), replace = TRUE)
for (rg in cfg$region_levels) {
  d <- streams[streams$REGION == rg, setdiff(names(streams), "REGION")]
  write_synth(d, file.path("records_w3/streams/streams_short",
                           paste0("REGION=", rg), "part-0.parquet"))
}

# =============================================================================
# 5. MANIFEST
# =============================================================================
message("Writing manifest ...")
manifest_path <- file.path(out_root, "README.md")
files <- list.files(out_root, recursive = TRUE)
files <- files[files != "README.md"]
manifest <- c(
  "# Synthetic data set for `reco_diversity`",
  "",
  "**SYNTHETIC, NON-IDENTIFYING DATA -- NOT REAL DATA.**",
  "",
  "Generated by `make_synthetic_data.R`. Every key is a fresh surrogate",
  "(no real `hashed_id`, artist id, song id or musicbrainz id is reused) and",
  "every value column is an independent resample of a real *marginal*",
  "distribution. No real individual's multivariate record is reproduced.",
  "",
  paste0("- Seed: `20240613`"),
  paste0("- Fidelity mode: `", cfg$realism, "`",
         if (cfg$realism == "panel")
           " (within-user structure + a positive recommendation/diversity link; estimates are plausibly signed but **arbitrary in magnitude** and do **not** reproduce the real findings)"
         else
           " (independent within-column resampling; FE/IV estimates are ~null/noise)"),
  paste0("- Users: ", cfg$n_users, " | periods: ", n_periods,
         " | artists: ", cfg$n_artists, " | songs: ", cfg$n_songs),
  paste0("- Generated: ", as.character(Sys.Date())),
  "",
  "## How to run the pipeline against it (offline, no S3)",
  "",
  "```sh",
  "RECO_DIVERSITY_DATA=synthetic Rscript -e 'targets::tar_make()'",
  "```",
  "",
  "The single switch is the `RECO_DIVERSITY_DATA` environment variable (see",
  "`R/common_functions.R::use_synthetic_data`). When set to `synthetic`,",
  "`initialize_s3()` returns a local-filesystem mock that reads this directory,",
  "and `_targets.R` stores intermediate targets locally instead of on S3.",
  "",
  "Switch fidelity mode when generating with",
  "`RECO_DIVERSITY_SYNTHETIC_REALISM=structural|panel`.",
  "",
  "## Files",
  ""
)
for (f in sort(files)) manifest <- c(manifest, paste0("- `", f, "`"))
writeLines(manifest, manifest_path)
message("Done. Synthetic data set written to ", out_root)
