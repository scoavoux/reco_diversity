#' List files with streaming data on AWS
list_streaming_data_files <- function(){
  s3 <- initialize_s3()
  stream_data_files <- s3$list_objects_v2(Bucket = "scoavoux", Prefix = "records_w3/streams")$Content %>% map(~.x$Key) %>% 
    unlist()
  stream_data_files <- stream_data_files[!str_detect(stream_data_files, "\\.keep")]
  return(stream_data_files)
}

#' Make user data
#' Only those in control group for now
make_user_data <- function(){
  s3 <- initialize_s3()
  users <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/RECORDS_hashed_user_group.parquet")$Body %>% 
    read_parquet()
  
  # restrict to users from control group
  users <- users %>% 
    filter(is_in_control_group, pay_offer) %>% 
    select(hashed_id) %>% 
    truncate_hashed_id()
  
  return(users)
}

#' Pairing between song_id and artist_id
make_items_data <- function(){
  require(tidytable)
  
  s3 <- initialize_s3()
  items_old <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/songs.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))
  items_new <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/items/song.snappy.parquet")$Body %>% 
    read_parquet(col_select = c("song_id", "artist_id"))  
  items <- bind_rows(items_old, items_new) %>% 
    filter(!is.na(artist_id)) %>% 
    distinct(song_id, .keep_all = TRUE)
  return(items)
}

#' Because importing them all at once creates memory problems, we divide
#' the task. This function loads and preprocesses each streaming data file
#' which is then turned to the next function for summary.
make_user_song_per_period_onefile <- function(file, users, interval = "month"){
  require(tidytable)
  require(lubridate)
  
  s3 <- initialize_s3()
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
    truncate_hashed_id() %>% 
    inner_join(users) %>% 
    filter(# filter only music played from 2017/01/01
           ts_listen >= 1483228800,
           is_listened == 1) %>% 
    mutate(ts_listen = as.integer(ts_listen)) %>% 
    mutate(period = breakdown_time(ts_listen, interval),
           lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
    select(-ts_listen, -listening_time, -is_listened)

  user_song_per_period <- streams %>% 
    summarise(l_play = sum(lt), 
              n_play = n(),
              .by = c(hashed_id, period, song_id, context_4))
  return(user_song_per_period)
}

merge_user_song_per_period <- function(...){
  require(tidytable)
  streams <- bind_rows(...) %>% 
    summarise(l_play = sum(l_play),
              n_play = sum(n_play),
              .by = c(hashed_id, period, song_id, context_4)) %>% 
    arrange(period)
  return(streams)
}

make_artists_to_remove <- function(artists_to_remove_file){
  artists_to_remove <- read_csv(artists_to_remove_file) %>% 
    select(artist_id)
  return(artists_to_remove)
}

## We bind each of the previous datasets together and compute summary stats.
make_user_artist_per_period <- function(user_song_per_period, items, artists_to_remove){
  require(tidytable)
  streams <- user_song_per_period %>% 
    inner_join(items) %>% 
    anti_join(artists_to_remove) %>% 
    group_by(hashed_id, period, artist_id, context_4) %>% 
    summarise(l_play = sum(l_play),
              n_play = sum(n_play)) %>% 
    arrange(period) %>% 
    mutate(period = factor(period))
  return(streams)
}

### Now do the same but at the song level to compute audio features based
### on spotify's audio features

make_items_acoustic_features_data <- function(items){
  require(tidytable)
  
  s3 <- initialize_s3()
  acoustic <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/250205-deezer-with-audio-ft.feather")$Body %>% 
    read_feather(col_select = c("song_id", "danceability", "energy", "loudness", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"))
  acoustic <- acoustic %>% 
    mutate(song_id = bit64::as.integer64(song_id))
  acoustic <- acoustic %>% 
    inner_join(select(items, song_id))
  return(acoustic)
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