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

# list all available files
list_streaming_data_files <- function(){
  s3 <- initialize_s3()
  stream_data_files <- s3$list_objects_v2(Bucket = "scoavoux", Prefix = "records_w3/streams")$Content %>% map(~.x$Key) %>% 
    unlist()
  stream_data_files <- stream_data_files[!str_detect(stream_data_files, "\\.keep")]
  return(stream_data_files)
}

make_items_data <- function(){
  require(tidyverse)
  require(tidytable)
  
  s3 <- initialize_s3()
  
  items_old <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/songs.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))
  items_new <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/song.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))  
  items <- bind_rows(items_old, items_new) %>% 
    distinct()
  return(items)
}


## Because importing them all at once creates memory problems, we divide
## the task. This function loads and preprocesses each streaming data file
## which is then turned to the next function for summary.
make_user_artist_per_period_table_onefile <- function(file, items, interval = "month"){
  require(tidyverse)
  require(tidytable)
  require(arrow)
  require(lubridate)
  
  breakdown_time <- function(time, interval){
    # year(time)
    if(interval == "month"){
      r <- format.Date(time, "%Y-%m")
    } else if(interval == "week"){
      r <- format.Date(time, "%Y-%W")
    }
    return(r)
  }
  
  s3 <- initialize_s3()
  users <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/RECORDS_hashed_user_group.parquet")$Body %>% 
     read_parquet()
  if(str_detect(file, "long")){
    streams <- s3$get_object(Bucket = "scoavoux", Key = file)$Body %>% 
      read_parquet(col_select = c("hashed_id", "ts_listen", "song_id",
                                  "is_listened", "listening_time", "context_4"))
  } else if(str_detect(file, "short")) {
    streams <- s3$get_object(Bucket = "scoavoux", Key = file)$Body %>% 
      read_parquet(col_select = c("hashed_id", "ts_listen", "media_id",
                                  "is_listened", "listening_time", "media_type", "context_4")) %>% 
      filter(media_type == "song") %>% 
      rename(song_id = "media_id") %>% 
      select(-media_type)
  }
  streams <- streams %>% 
    filter(hashed_id %in% filter(users, is_in_control_group)$hashed_id,
           # filter only music played from 2017/01/01
           ts_listen >= 1483228800,
           is_listened == 1) %>% 
    mutate(ts_listen = as.integer(ts_listen)) %>% 
    mutate(period = breakdown_time(ts_listen, interval),
           lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
    select(-ts_listen, -listening_time)
  
  
  user_artist_per_period <- streams %>% 
    left_join(items) %>% 
    filter(!is.na(artist_id)) %>% 
    summarise(l_play = sum(lt), 
              n_play = n(),
              .by = c(hashed_id, period, artist_id, context_4))
  return(user_artist_per_period)
}

make_artists_to_remove <- function(artists_to_remove_file){
  artists_to_remove <- read_csv(artists_to_remove_file) %>% 
    select(artist_id)
  return(artists_to_remove)
}

## We bind each of the previous datasets together and compute summary stats.
merge_user_artist_per_period_table <- function(..., artists_to_remove){
  library(tidyverse)
  library(tidytable)
  streams <- bind_rows(...) %>% 
    summarise(l_play = sum(l_play),
              n_play = sum(n_play),
              .by = c(hashed_id, period, artist_id, context_4)) %>% 
    arrange(period) %>% 
    mutate(period = factor(period)) %>% 
    anti_join(artists_to_remove)
  return(streams)
}

make_genre_data <- function(){
  s3 <- initialize_s3()
  artists_old <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/artists_data.snappy.parquet")$Body %>% 
    read_parquet(col_select = 1:3)
  artists_new <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/artist.snappy.parquet")$Body %>% 
    read_parquet(col_select = 1:3)
  genres <- bind_rows(artists_new, artists_old) %>% 
    filter(!is.na(main_genre)) %>% 
    mutate(genre = recode_vars(main_genre, "deezer_maingenre")) %>% 
    distinct(artist_id, genre) %>% 
    slice(1, .by = "artist_id")
  return(genres)
}


make_artists_pop <- function(){
  s3 <- initialize_s3()
  artists_pop <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/artists_pop.csv")$Body %>% 
    read_csv(col_select = c("artist_id", "nb_fans"))
  artists_pop <- artists_pop %>% filter(!is.na(nb_fans))
  return(artists_pop)
}

make_unique_artists <- function(user_artist_per_period){
  require(arrow)
  ua <- user_artist_per_period %>% 
    distinct(artist_id)
  s3 <- initialize_s3()
  ar1 <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/artist.snappy.parquet")$Body %>% 
    read_parquet(col_select = c(1,2))
  ar2 <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/artists_data.snappy.parquet")$Body %>% 
    read_parquet(col_select = c(1,2))
  ar <- bind_rows(ar2, ar1) %>% 
    distinct()
  ua <- left_join(ua, ar)
  return(ua)
}

export_unique_artists <- function(unique_artists){
  filename <- "data/temp/unique_artists.csv"
  unique_artists %>% 
    write_csv(filename)
  return(filename) 
}

make_artists_gender <- function(unique_artists){
  # 
  s3 <- initialize_s3()
  gender <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbz_gender.csv")$Body %>% 
    read_csv() %>% 
    rename(mbid = "gid")
  mbid <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_deezerid.csv")$Body %>% 
    read_csv()
  res <- inner_join(unique_artists, inner_join(mbid, gender)) %>% 
    select(artist_id, gender) %>% 
    slice(1, .by = artist_id)
  return(res)
}

make_artists_area <- function(unique_artists){
  # 
  s3 <- initialize_s3()
  mbid <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_deezerid.csv")$Body %>% 
    read_csv()
  area <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_area.csv")$Body %>% 
    read_csv() %>% 
    rename(mbid = "gid", area_id = "area")
  area_names <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/area_names.csv")$Body %>% 
    read_csv() %>% 
    rename(area_id = "id", area_name = "name")
  ## check out area types also
  ## we restrict to countries:
  area_names <- area_names %>% 
    filter(type == 1)
  area <- inner_join(area, area_names)
  res <- inner_join(unique_artists, mbid) %>% 
    inner_join(area) %>% 
    slice(1, .by = artist_id) %>% 
    select(artist_id, area_name)
  return(res)
}