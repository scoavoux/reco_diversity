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

compute_linguistic_diversity <- function(user_artist_per_period, language){
  linguistic_div <- user_artist_per_period %>% 
    left_join(language) %>% 
    group_by(hashed_id, period, lang) %>% 
    summarize(l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l)) %>% 
    summarize(div_linguistic = compute_div(f))
  return(linguistic_div)
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


# retired function
# compute_pop_diversity <- function(user_artist_per_period, 
#                                   artists_pop,
#                                   long_tail_quantile=.9){
#   long_tail_limit <- quantile(artists_pop$nb_fans, long_tail_quantile)
#   pop_div <- user_artist_per_period %>% 
#     inner_join(artists_pop) %>% 
#     group_by(hashed_id, period) %>% 
#     mutate(f = l_play / sum(l_play)) %>% 
#     summarize(mean_pop = sum(f*nb_fans),
#               f_longtail = sum(nb_fans < long_tail_limit) / n(),
#               nb_longtail_pond = sum(f*(nb_fans < long_tail_limit)))
#   return(pop_div)
# }

compute_endo_pop_diversity <- function(user_artist_per_period, 
                                       long_tail_limit = .9,
                                       superstar_limit = .99){
  # for each artist/period...
  # the number of unique organic consumers per year
  uu <- user_artist_per_period %>% 
    filter(context_4 == "organic") %>% 
    mutate(year = str_extract(period, "^\\d{4}") %>% as.numeric()) %>% 
    filter(!is.na(year)) %>% 
    group_by(artist_id, year) %>% 
    summarize(n = sum(l_play)/(60*60))
  
  th <- uu %>% 
    group_by(year) %>% 
    summarize(longtail_th = quantile(n, long_tail_limit),
              superstar_th = quantile(n, superstar_limit))
  
  artist_period_starcat <- uu %>% 
    left_join(th) %>% 
    group_by(year) %>% 
    mutate(starcat = case_when(n <= longtail_th ~ "longtail",
                               n <= superstar_th ~ "intermediate",
                               n > superstar_th ~ "superstar") %>% 
             factor(levels = c("longtail", "intermediate", "superstar"))) %>% 
    ungroup() %>% 
    select(artist_id, year, starcat)
  
  # now lag that
  # and compute weighted mean for each user/period
  x <- user_artist_per_period %>% 
    group_by(hashed_id, period, artist_id) %>% 
    summarize(l = sum(l_play)) %>% 
    filter(l > 0) %>% 
    left_join(artist_period_starcat) %>% 
    mutate(starcat = fct_na_value_to_level(starcat, level = "not_organic")) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l/sum(l)) %>% 
    group_by(hashed_id, period, starcat) %>% 
    summarize(f_endo = sum(f),
              n = n()) %>% 
    ungroup()
  endopop_div_f <- x %>% 
    select(-n) %>% 
    pivot_wider(names_from = starcat, names_prefix = "f_endo_", values_from = f_endo, values_fill = 0)
  endopop_div_n <- x %>% 
    select(-f_endo) %>% 
    pivot_wider(names_from = starcat, names_prefix = "n_endo_", values_from = n, values_fill = 0)
  endopop_div <- full_join(endopop_div_f, endopop_div_n)
  return(endopop_div)
}

compute_gender_diversity <- function(user_artist_per_period, 
                                     gender,
                                     min_nonmissing_n = 5,
                                     min_nonmissing_freq = .3){
  # We restrict the dataset to only sessions with enough nonmissing
  # artist gender.
  # Default minimum is 5 artists with a gender, making at least 30%
  # of total listening time
  gender_div <- user_artist_per_period %>% 
    filter(l_play > 0) %>% 
    left_join(gender) %>% 
    filter(is.na(gender) | gender == 1 | gender == 2) %>% 
    group_by(hashed_id, period, gender) %>% 
    summarize(n = n(),
              l = sum(l_play)) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l)) %>% 
    filter(!is.na(gender)) %>% 
    mutate(keep = ifelse(sum(n) >= min_nonmissing_n & sum(f) >= min_nonmissing_freq,
                         TRUE,
                         FALSE)) %>% 
    ungroup() %>% 
    filter(keep) %>% 
    select(hashed_id, period, gender, f) %>% 
    mutate(gender = factor(gender, c(1,2), c("f_men", "f_women"))) %>% 
    pivot_wider(names_from = gender, values_from = f, values_fill = 0) %>% 
    select(hashed_id, period, f_women)
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

compute_release_recency <- function(user_artist_per_period, release){
  average_artist_age <- user_artist_per_period %>% 
    inner_join(release) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l_play / sum(l_play)) %>% 
    summarize(average_artist_age = sum(date_begin * f))
  return(average_artist_age)
}