#' create the interface with Datalab/INSEE AWS
initialize_s3 <- function(){
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
