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
    group_by(hashed_id, period) %>% 
    mutate(f = l_play / sum(l_play)) %>% 
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

compute_gender_diversity <- function(user_artist_per_period, gender){
  gender_div <- user_artist_per_period %>% 
    inner_join(gender) %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l_play / sum(l_play)) %>% 
    summarize(f_woman = sum(f * gender == 2))
  return(gender_div)
}

compute_legitimacy_diversity <- function(user_artist_per_period){}

make_user_period_level_data <- function(..., 
                                        min_hours_played = 2, 
                                        min_artist_played = 1){
  l <- list(...)
  users_raw <- l[[1]]
  for(i in 2:length(l)){
    users_raw <- users_raw %>% 
      full_join(l[[i]], by = c("hashed_id", "period"))
  }
  # we add a constraint: to keep a user
  users_raw <- users_raw %>% 
    filter(total_play_l > min_hours_played,
           n_artist > min_artist_played)
  
  return(users_raw)
}