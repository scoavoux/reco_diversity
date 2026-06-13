#' Single switch: are we running on synthetic, offline data?
#'
#' Controlled by the environment variable `RECO_DIVERSITY_DATA`.
#' - unset or "real"      -> read the real data from the S3 server (default)
#' - "synthetic"          -> read the local, non-identifying synthetic copy
#'                           produced by make_synthetic_data.R
#'
#' This is the only switch downstream code needs: every data-loading function
#' goes through `initialize_s3()`, which returns either the real S3 client or a
#' local filesystem mock that behaves like it.
use_synthetic_data <- function(){
  tolower(Sys.getenv("RECO_DIVERSITY_DATA", "real")) == "synthetic"
}

#' Root directory of the synthetic data set.
#' It mirrors the real S3 bucket layout: a file with S3 key
#' "records_w3/items/song.snappy.parquet" lives at
#' file.path(synthetic_data_dir(), "records_w3/items/song.snappy.parquet").
synthetic_data_dir <- function(){
  Sys.getenv("RECO_DIVERSITY_SYNTHETIC_DIR", here::here("data", "synthetic"))
}

#' The real interface with Datalab/INSEE AWS.
#' Always returns the live S3 client, regardless of the switch above. Used by
#' make_synthetic_data.R, which must read the *real* data to derive the
#' synthetic copy.
initialize_s3_real <- function(){
  s3 <- paws::s3(config = list(
    credentials = list(
      creds = list(
        access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
        session_token = Sys.getenv("AWS_SESSION_TOKEN")
      )),
    endpoint = paste0("https://", Sys.getenv("AWS_S3_ENDPOINT")),
    region = Sys.getenv("AWS_DEFAULT_REGION")))

  return(s3)
}

#' A drop-in, local-filesystem replacement for the paws S3 client.
#'
#' It implements just the two methods the pipeline uses:
#'  - get_object(Bucket, Key)$Body         -> raw vector of the file's bytes
#'  - list_objects_v2(Bucket, Prefix)$Content -> list of list(Key = ...)
#'
#' Because `$Body` is a raw vector, exactly like the real paws client, every
#' existing `s3$get_object(...)$Body %>% read_parquet()/read_csv()/read_feather()`
#' call site keeps working unchanged: arrow and readr both accept raw vectors.
local_s3_mock <- function(root = synthetic_data_dir()){
  list(
    get_object = function(Bucket, Key){
      # Tolerate an accidental leading slash in a few keys (e.g. omnivorism)
      Key <- sub("^/+", "", Key)
      path <- file.path(root, Key)
      if(!file.exists(path)){
        stop("Synthetic data file not found: ", path,
             "\nDid you run make_synthetic_data.R to generate it?")
      }
      list(Body = readBin(path, what = "raw", n = file.info(path)$size))
    },
    list_objects_v2 = function(Bucket, Prefix){
      dir <- file.path(root, Prefix)
      if(!dir.exists(dir)){
        return(list(Content = list()))
      }
      rel <- list.files(dir, recursive = TRUE, full.names = FALSE)
      keys <- file.path(Prefix, rel)
      list(Content = lapply(keys, function(k) list(Key = k)))
    }
  )
}

#' create the interface with Datalab/INSEE AWS.
#' Honors the synthetic-data switch (see `use_synthetic_data()`): in synthetic
#' mode it returns a local filesystem mock so the whole pipeline runs offline.
initialize_s3 <- function(){
  if(use_synthetic_data()){
    return(local_s3_mock())
  }
  initialize_s3_real()
}

#' Change variable levels to their long, human readable, pretty form
#' for figures and tables. All correspondances are in codes.csv
 
recode_vars <- function(char, .scheme){
  e <- readr::read_csv(here::here("data", "codes.csv"), col_types = "ccc")
  e <- dplyr::filter(e, scheme == .scheme)
  val <- e$replacement
  names(val) <- e$orig
  return(val[char])
}


#' Simplify hashed_id for storage
#' hashed_id is 64 characters long. The first 8 characters are
#' unique (empirically, in our dataset) => we truncate the
#' variable to save space
truncate_hashed_id <- function(df){
  mutate(df, hashed_id = str_extract(hashed_id, "^\\w{9}"))
}

#' Breakdown time in intervals
breakdown_time <- function(time, interval){
  # year(time)
  if(interval == "month"){
    r <- format.Date(time, "%Y-%m")
  } else if(interval == "week"){
    r <- format.Date(time, "%Y-%W")
  }
  return(r)
}
