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
    filter(# filter only music played from 2019/01/01
      # there are almost no data before that date
           ts_listen >= 1546300800,
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
    ungroup() %>% 
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
    inner_join(select(items, song_id)) %>% 
    distinct()
  return(acoustic)
}

make_acoustic_features_pca <- function(acoustic_features){
  pc <- irlba::prcomp_irlba(select(acoustic_features, -song_id), scale. = TRUE, n = 5, center = TRUE)
  row.names(pc$rotation) <- names(acoustic_features)[2:10]
  return(pc)
}

make_acoustic_features_pca_data <- function(acoustic_features_pca, acoustic_features){
  ac <- acoustic_features %>% 
    select(song_id) %>% 
    mutate(pc1 = acoustic_features_pca$x[,"PC1"],
           pc2 = acoustic_features_pca$x[,"PC2"],
           pc3 = acoustic_features_pca$x[,"PC3"],
           pc4 = acoustic_features_pca$x[,"PC4"])
  return(ac)
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

make_artists_language <- function(unique_artists){
  s3 <- initialize_s3()
  language <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/artists_songs_languages.csv")$Body %>% 
    read_csv() %>% 
    rename(artist_id = 'art_id') %>% 
    select(-nb_songs) %>% 
    right_join(unique_artists)
  return(language)
}

make_artists_release <- function(unique_artists){
  s3 <- initialize_s3()
  
  # Musicbrainz id to deezer id
  mbid <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_deezerid.csv")$Body %>% 
    read_csv() %>% 
    right_join(select(unique_artists, artist_id))
    
  
  # Data about dates of release of albums
  release_group <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_release_group.csv")$Body %>% 
    read_csv()
  
  date_begin <- release_group %>% 
    right_join(mbid) %>% 
    filter(!is.na(first_release_date_year)) %>% 
    group_by(artist_id) %>% 
    arrange(first_release_date_year) %>% 
    slice(1) %>% 
    select(artist_id, date_begin = first_release_date_year) %>% 
    ungroup() %>% 
    mutate(date_begin = ifelse(date_begin < 1960, 1960, date_begin))
  return(date_begin)
}

## We would 
make_user_context4_onefile <- function(file, interval = "month"){
  require(tidytable)
  require(lubridate)
  s3 <- initialize_s3()
  if(str_detect(file, "long")){
    streams <- s3$get_object(Bucket = "scoavoux", Key = file)$Body %>% 
      read_parquet(col_select = c("hashed_id", "ts_listen", 
                                  "is_listened", "listening_time", "context_4"))
  } else if(str_detect(file, "short")) {
    streams <- s3$get_object(Bucket = "scoavoux", Key = file)$Body %>% 
      read_parquet(col_select = c("hashed_id", "ts_listen", 
                                  "is_listened", "listening_time", "media_type", "context_4")) %>% 
      filter(media_type == "song") %>% 
      select(-media_type)
  }
  streams <- streams %>%
    truncate_hashed_id() %>% 
    filter(# filter only music played from 2017/01/01
      ts_listen >= 1483228800,
      is_listened == 1) %>% 
    mutate(ts_listen = as.integer(ts_listen)) %>% 
    mutate(period = breakdown_time(ts_listen, interval),
           lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
    select(-ts_listen, -listening_time, -is_listened)
  
  user_context_per_period <- streams %>% 
    summarise(l_play = sum(lt), 
              n_play = n(),
              .by = c(hashed_id, period, context_4))
  return(user_context_per_period)
}

make_artists_cluster <- function(){
  require(igraph)
  s3 <- initialize_s3()
  related <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/related_artists/related_artists.csv")$Body %>% 
    read_csv()
  related_graph <- graph_from_data_frame(select(related, orig_artist_id, dest_artist_id), directed=TRUE)
  cl <- cluster_infomap(related_graph)
  
  clusters <- tibble(
    vertex_id  = V(related_graph)$name,
    cluster_id = membership(cl))
  results <- clusters %>% 
    rename(orig_artist_id = vertex_id) %>% 
    mutate(orig_artist_id = as.numeric(orig_artist_id)) %>% 
    right_join(distinct(related, orig_artist_id)) %>% 
    add_count(cluster_id) %>% 
    mutate(cluster_id = ifelse(n < 10, NA, cluster_id)) %>% 
    select(artist_id = "orig_artist_id", related_artists_infomap_cluster = "cluster_id")
  return(results)
}

# Compute baseline for Bartik IV ------
make_recoshare_instrument <- function(user_reco,
                                    week_threshold = if (use_synthetic_data()) as.integer(Sys.getenv("RECO_DIVERSITY_BASELINE_MIN_WEEKS", "8")) else 20,
                                    volume_threshold = if (use_synthetic_data()) as.numeric(Sys.getenv("RECO_DIVERSITY_BASELINE_MIN_HOURS", "10")) else 100){
  # First, compute the baseline for each user
  # Baseline is share of reco the first year (2019)
  # We restrict the dataset to users
  # - who streamed on 20 weeks
  # - in 2019
  # - for a total of 100 hours at least
  
  user_reco_2019 <- user_reco %>% 
    mutate(year = str_extract(period, "^\\d{4}") %>% as.integer()) %>% 
    filter(year == 2019, total_play_l > 0) %>% 
    select(-c4_organic)
  
  users_to_include <- user_reco_2019 %>% 
    group_by(hashed_id) %>% 
    summarize(n = n(), 
              l = sum(total_play_l)) %>% 
    filter(n > week_threshold,
           l > volume_threshold) %>% 
    select(hashed_id)
  
  baseline <- users_to_include %>% 
    left_join(user_reco_2019) %>% 
    group_by(hashed_id) %>% 
    summarize(across(c4_edito:c4_reco, 
                     ~sum(.x*total_play_l, na.rm=TRUE)/sum(total_play_l, na.rm=TRUE),
                     .names = "baseline_{.col}")) %>% 
    ungroup()
  
  # Next, compute the shift, considered as deviation from average
  user_reco <- user_reco %>% 
    select(-c4_organic) %>% 
    group_by(period) %>% 
    mutate(den_t = sum(total_play_l),
           across(starts_with("c4_"), 
                  ~ (sum(total_play_l * .x, na.rm=TRUE) - (total_play_l * .x)) / (den_t - total_play_l),
                  .names = "shift_{.col}")) %>% 
    ungroup() %>% 
    select(-den_t)
  
  # Finally, we compute the instrument. It is only valid (not NA) if
  # - user has a baseline (see restriction above)
  # - year is post 2019 (because that's when the baseline is computed)
  
  res <- user_reco %>% 
    inner_join(baseline, by = "hashed_id") %>% 
    mutate(year = str_extract(period, "^\\d{4}") %>% as.integer()) %>% 
    filter(year > 2019, total_play_l > 0) %>% 
    mutate(                                                                            
      Z_c4_reco      = baseline_c4_reco      * shift_c4_reco,
      Z_c4_edito     = baseline_c4_edito     * shift_c4_edito,                         
      Z_c4_reco_algo = baseline_c4_reco_algo * shift_c4_reco_algo
    )                                                                                  
  res <- select(res, hashed_id, period, 
                starts_with("Z_"),
                starts_with("baseline_"),
                starts_with("shift_"))
  return(res)
}

