compute_descriptive_stats <- function(user_artist_per_period){
  require(kableExtra)
  options(scipen = 99)
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

plot_dependant_variables_density <- function(user_period_div, .transformation = "raw"){
  mp <- yaml::read_yaml("data/model_params.yaml") %>% 
    bind_rows() %>% 
    distinct(diversity, log, scale)
  if(.transformation != "raw"){
    for(i in 1:nrow(mp)){
      div_var <- sym(mp$diversity[i])
      
      if(mp$log[i]){
        user_period_div <- user_period_div %>%
          mutate({{ div_var }} := log({{ div_var }} + 1))
      }
      if(mp$scale[i]){
        user_period_div <- user_period_div %>%
          mutate({{ div_var }} := scale({{ div_var }}))
      }
    }
  }
  x <- select(user_period_div, all_of(mp$diversity)) %>% 
    pivot_longer(everything()) %>% 
    mutate(name = recode_vars(name, "cleandiversity"))
  
  theme_set(theme_minimal())
  
  gg <- ggplot(x, aes(value)) +
    geom_density() +
    facet_wrap(~name, scale = "free")
  filename <- str_glue("output/dependant_density_{.transformation}.pdf")
  ggsave(filename, gg, width = 12, height = 10)
  return(filename)
}

context_by_social_status <- function(user_context4_onefile){
  s3 <- initialize_s3()
  users <- s3$get_object(Bucket = "scoavoux", Key = "records_w3/RECORDS_hashed_user_group.parquet")$Body %>% 
    read_parquet()
  
  # restrict to users from respondent group
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


plot_recommendation_use <- function(user_period_div){
  upd <- user_period_div %>% select(hashed_id, period, total_play_l, starts_with("c4"))
  abs_change <- upd %>% 
    group_by(hashed_id) %>% 
    mutate(c4_reco_absolute_change = abs(c4_reco - lag(c4_reco))) %>% 
    filter(!is.na(c4_reco_absolute_change))
  # this plots the share of periods that have seen a change in recommendation 
  # use by various thresholds (5, 10, 25, 50 pp).
  theme_set(theme_minimal(base_size = 15))
  
  gg <- abs_change %>% 
    group_by(hashed_id) %>% 
    summarize(n = n(),
              `More than 05 pp change` = sum(c4_reco_absolute_change > .05) / n,
              `More than 10 pp change` = sum(c4_reco_absolute_change > .1) / n,
              `More than 25 pp change` = sum(c4_reco_absolute_change > .25) / n,
              `More than 50 pp change` = sum(c4_reco_absolute_change > .5) / n) %>% 
    pivot_longer(starts_with("More")) %>% 
    ggplot(aes(value)) +
      geom_density() +
      facet_wrap(~name, scale = "free_y") +
      labs(x = "Period-to-period change in use of recommendation",
           y = "")
 filename <- "output/gg_change_recommendation_use.pdf"   
 ggsave(filename, gg)
 return(filename)
}
