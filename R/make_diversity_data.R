compute_div <- function(.x){
  return(prod(.x^.x)^-1)
}

compute_use_of_recommendations <- function(user_artist_per_period){
  device <- user_artist_per_period %>% 
    filter(context_4 %in% c("organic", "reco_algo", "edito")) %>% 
    group_by(hashed_id, period, context_4) %>% 
    summarize(l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l),
           total_play_l = sum(l)/(60*60),
           context_4 = paste0("c4_", context_4)) %>% 
    select(-l) %>% 
    pivot_wider(names_from = context_4, values_from = f, values_fill = 0) %>% 
    mutate(c4_reco = c4_reco_algo + c4_edito)
  return(device)
}

compute_artist_diversity <- function(user_artist_per_period){
  artist_div <- user_artist_per_period %>% 
    group_by(hashed_id, period, artist_id) %>% 
    summarize(l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l)) %>% 
    summarize(div_artist = compute_div(f),
              n_artist   = n())
  return(artist_div)
}

compute_regional_diversity <- function(user_artist_per_period, area){
  regional_div <- user_artist_per_period %>% 
    left_join(area) %>% 
    group_by(hashed_id, period, area_name) %>% 
    summarize(l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l)) %>% 
    summarize(div_regional = compute_div(f))
  return(regional_div)
}

## TODO: implement cultural holes

compute_genre_diversity <- function(user_artist_per_period, genres){
  genre_div <- user_artist_per_period %>% 
    inner_join(genres) %>% 
    group_by(hashed_id, period, genre) %>% 
    summarize(l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l)) %>% 
    summarize(div_genre = compute_div(f),
              n_genre   = n())
  return(genre_div)
}

compute_pop_diversity <- function(user_artist_per_period, 
                                  artists_pop,
                                  long_tail_quantile=.9){
  long_tail_limit <- quantile(artists_pop$nb_fans, long_tail_quantile)
  pop_div <- user_artist_per_period %>% 
    inner_join(artists_pop) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l_play / sum(l_play)) %>% 
    summarize(mean_pop = sum(f*nb_fans),
              f_longtail = sum(nb_fans < long_tail_limit) / n(),
              nb_longtail_pond = sum(f*(nb_fans < long_tail_limit)))
  return(pop_div)
}

compute_endo_pop_diversity <- function(user_artist_per_period, long_tail_limit = .9){
  # for each artist/period...
  # the number of unique consumers 
  uu <- user_artist_per_period %>% 
    distinct(hashed_id, artist_id, period) %>% 
    count(artist_id, period) %>% 
    mutate(n_prev = lag(n)) %>% 
    filter(!is.na(n_prev)) %>% 
    select(-n)
  # now lag that
  # and compute weighted mean for each user/period
  endopop_div <- user_artist_per_period %>% 
    group_by(hashed_id, period, artist_id) %>% 
    summarize(l = sum(l_play)) %>% 
    left_join(uu) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l/sum(l),
           long_tail_limit_threshold = quantile(n_prev, long_tail_limit, na.rm=TRUE)) %>% 
    summarize(mean_unique_users = sum(f * n_prev),
              f_endo_longtail = sum(n_prev < long_tail_limit_threshold, na.rm = TRUE) / n(),
              nb_endo_longtail_pond = sum(f*(n_prev < long_tail_limit_threshold), na.rm=TRUE))
  return(endopop_div)
}

compute_gender_diversity <- function(user_artist_per_period, gender){
  gender_div <- user_artist_per_period %>% 
    inner_join(gender) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l_play / sum(l_play)) %>% 
    summarize(f_woman = sum(f * gender == 2))
  return(gender_div)
}

compute_acoustic_diversity <- function(user_song_per_period, acoustic_features){
  require(tidytable)
  acoustic_diversity <- user_song_per_period %>% 
    group_by(hashed_id, period, song_id) %>% 
    summarize(l = sum(l_play)) %>% 
    ungroup() %>% 
    inner_join(acoustic_features, by = "song_id") %>% 
    add_count(hashed_id, period) %>% 
    filter(n > 1, l > 0) %>% 
    group_by(hashed_id, period) %>% 
    summarize(across(danceability:tempo, list(mean = ~ Hmisc::wtd.mean(.x, l, normwt = TRUE), 
                                              sd   = ~ sqrt(Hmisc::wtd.var(.x, l, normwt = TRUE))
                                              )
                     )
              )
    return(acoustic_diversity)
}

compute_legitimacy_diversity <- function(user_artist_per_period, artist_legitimacy){
  require(tidytable)
  s3 <- initialize_s3()
  artists <- s3$get_object(Bucket = "scoavoux", Key = "omnivorism/objects/artists_csv")$Body %>% 
    read_csv() %>% 
    select(artist_id, starts_with("sc_"))
  
  omnivore_diversity <- user_artist_per_period %>%
    group_by(hashed_id, period, artist_id) %>% 
    summarize(l = sum(l_play, na.rm=TRUE)) %>%
    ungroup() %>% 
    left_join(artists) %>% 
    group_by(hashed_id, period) %>% 
    summarize(across(starts_with("sc_"), 
                     list(mean = ~Hmisc::wtd.mean(.x, weights = l, na.rm = TRUE),
                          sd   = ~sqrt(Hmisc::wtd.var(.x, weights = l, na.rm = TRUE))))) %>% 
    ungroup()
  return(omnivore_diversity)
}

make_user_period_level_data <- function(..., 
                                        min_hours_played = 2, 
                                        min_artist_played = 1,
                                        min_users_per_period = 1000,
                                        trim_first_week = TRUE){
  l <- list(...)
  users_raw <- l[[1]]
  for(i in 2:length(l)){
    users_raw <- users_raw %>% 
      full_join(l[[i]], by = c("hashed_id", "period"))
  }
  
  # We filter out periods before june 2018 for lack of users
  # We do it here because compiling the data was long and
  # I don't want to go through it again but it should be done 
  # in a previous step (make_songs_users data)
  users_raw <- users_raw %>%
    ungroup() %>% 
    add_count(period) %>% 
    filter(n >= min_users_per_period) %>% 
    select(-n)
  if(trim_first_week){
    users_raw <- users_raw %>% 
      filter(!(str_detect(period, "-00")))
  }
  # We add a constraint: to keep a user
  users_raw <- users_raw %>% 
    ungroup() %>% 
    filter(total_play_l > min_hours_played,
           n_artist > min_artist_played)
  
  return(users_raw)
}