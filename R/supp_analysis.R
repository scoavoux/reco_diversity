compute_descriptive_stats <- function(user_artist_per_period){
  require(kableExtra)
  tb <- tibble(unique_users = n_distinct(user_artist_per_period$hashed_id),
               unique_periods = n_distinct(user_artist_per_period$period),
               unique_listening_events = sum(user_artist_per_period$n_play, na.rm = TRUE),
               unique_artists = n_distinct(user_artist_per_period$artist_id),
               mean_periods_per_user = nrow(distinct(user_artist_per_period, hashed_id, period))/unique_users)
  filename <- "output/stat_desc.tex"
  tb %>% 
    pivot_longer(everything()) %>% 
    kbl(format = "latex", digits = 2, booktabs = TRUE) %>% 
    save_kable(file = filename)
  return(filename)
}

context_by_social_status <- function(user_context4_onefile){
  s3 <- initialize_s3()
  users <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/RECORDS_hashed_user_group.parquet")$Body %>% 
    read_parquet()
  
  # restrict to users from control group
  users <- users %>% 
    filter(is_respondent , pay_offer) %>% 
    select(hashed_id) %>% 
    truncate_hashed_id()
  
  survey <- s3$get_object(Bucket = "scoavoux", Key = "/omnivorism/objects/users_csv")$Body %>% 
    read_csv() %>% 
    select(hashed_id, age, gender, degree) %>% 
    truncate_hashed_id()
  survey
  
  context_periods <- user_context4_onefile %>% 
    mutate(period = ym(period)) %>% 
    filter(period > ym("2018-05")) %>% 
    inner_join(users) %>% 
    group_by(hashed_id, period, context_4) %>% 
    summarize(l = sum(l_play, na.rm=TRUE),
              n = sum(n_play, na.rm=TRUE)) %>% 
    ungroup()
  reco <- context_periods %>% 
    group_by(hashed_id, period) %>% 
    mutate(f = l / sum(l, na.rm=TRUE)) %>% 
    filter(context_4 %in% c("edito", "reco_algo")) %>% 
    left_join(survey)
  
  reco %>%
    filter(!is.na(degree)) %>% 
    ggplot(aes(period, f, color = degree)) +
      stat_summary() +
      facet_wrap(~context_4)
  
  filter(reco, !is.na(gender)) %>% 
    ggplot(aes(period, f, color = gender)) +
      stat_summary() +
      facet_wrap(~context_4)
  
}