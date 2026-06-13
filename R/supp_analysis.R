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


plot_recommendation_use_change <- function(user_period_div){
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
 filename <- "output/gg_recommendation_use_change.pdf"   
 ggsave(filename, gg)
 return(filename)
}

plot_context_ternary <- function(user_period_div){
  require(ggtern)
  upd <- select(user_period_div, period, hashed_id, c4_edito, c4_reco_algo, c4_organic)

  u <- upd %>% 
    group_by(hashed_id) %>% 
    summarize(across(starts_with("c4"), ~mean(.x))) 
  theme_set(theme_minimal())
  gg <- u %>% 
    ggtern(aes(x = c4_edito, y = c4_organic, z = c4_reco_algo)) +
      geom_point(shape = ".") +
      labs(x = "Editorial", y = "Organic", z = "Algorithm")
  filename <- "output/gg_ternary_context_use.png"
  ggsave(filename, gg)
  return(filename)
}

plot_recommendation_use_by_year <- function(user_period_div){
  theme_set(theme_minimal(base_size = 15))
  yty <- user_period_div %>% 
    select(-c4_reco) %>% 
    mutate(year = str_extract(period, "\\d{4}") %>% as.numeric()) %>% 
    group_by(year, hashed_id) %>% 
    summarize(across(starts_with("c4_"), mean)) %>% 
    summarize(across(starts_with("c4_"), mean))
  
  
  gg <- yty %>% 
    pivot_longer(-year) %>% 
    mutate(name = factor(name, levels = c("c4_organic", "c4_edito", "c4_reco_algo"),
                         labels = c("Organic", "Editorial", "Algorithmic"))) %>% 
    ggplot(aes(year, value, color = name)) +
      geom_point() +
      geom_line() +
      labs(x = "", y = "", color = "Context") +
      scale_y_continuous(breaks = seq(0, .8, .2), limits = c(0, .75))
  filename <- "output/gg_recommendation_use_by_year.png"
  ggsave(filename, gg, width = 10)
  return(filename)
  
}

plot_algorithms_use_by_genre_year <- function(user_artist_per_period, genres, .by_year = TRUE){
  theme_set(theme_minimal(base_size = 15))
  if(.by_year){
    d <- user_artist_per_period %>% 
      filter(context_4 %in% c("organic", "reco_algo", "edito")) %>% 
      mutate(year = str_extract(period, "\\d{4}")) %>% 
      group_by(year, artist_id, context_4) %>% 
      summarize(l = sum(l_play)) %>% 
      left_join(genres) %>% 
      group_by(year, context_4, genre) %>% 
      summarize(l = sum(l)) %>% 
      ungroup() %>% 
      group_by(year, genre) %>% 
      mutate(f = l / sum(l)) %>% 
      ungroup()
    genre_levels <- d %>% 
      filter(context_4 == "organic", year == min(year, na.rm = TRUE), !is.na(genre)) %>% 
      arrange(f) %>% 
      mutate(genre = recode_vars(genre, "cleangenres")) %>% 
      pull(genre)
    d <- d %>% 
      mutate(genre = recode_vars(genre, "cleangenres") %>% factor(levels = genre_levels),
             context_4 = factor(context_4, 
                                levels = c("organic", "edito", "reco_algo"),
                                labels = c("Organic", "Editorial", "Algorithm"))) %>% 
      filter(!is.na(genre), !is.na(context_4), !is.na(year))
  } else if(!.by_year){
    d <- user_artist_per_period %>% 
      filter(context_4 %in% c("organic", "reco_algo", "edito")) %>% 
      group_by(artist_id, context_4) %>% 
      summarize(l = sum(l_play)) %>% 
      left_join(genres) %>% 
      group_by(context_4, genre) %>% 
      summarize(l = sum(l)) %>% 
      ungroup() %>% 
      group_by(genre) %>% 
      mutate(f = l / sum(l)) %>% 
      ungroup()
    genre_levels <- d %>% 
      filter(context_4 == "organic", !is.na(genre)) %>% 
      arrange(f) %>% 
      mutate(genre = recode_vars(genre, "cleangenres")) %>% 
      pull(genre)
    d <- d %>% 
      mutate(genre = recode_vars(genre, "cleangenres") %>% factor(levels = genre_levels),
             context_4 = factor(context_4, 
                                levels = c("organic", "edito", "reco_algo"),
                                labels = c("Organic", "Editorial", "Algorithm"))) %>% 
      filter(!is.na(genre), !is.na(context_4))
    
  }
  gg <- ggplot(d, aes(f, genre, fill = context_4)) +
    geom_col(position = "fill") +
    scale_fill_brewer(palette = 'Set2') +
    labs(x = "", y = "",
         fill = "Context")
  
  if(.by_year){
    gg <- gg + facet_wrap(~year)
  }
  
  yearchar <- ifelse(.by_year, "_by_year", "_global")
  filename <- str_glue("output/gg_algorithms_use_by_genre_year{yearchar}.png")
  ggsave(filename, gg, width = 12)
  return(filename)
  
}


plot_recommendation_use_rythms <- function(){
  if(use_synthetic_data()){
    # Offline mode: read the local synthetic streams dataset instead of S3.
    data_cloud <- arrow::open_dataset(
      source = file.path(synthetic_data_dir(), "records_w3/streams/streams_short"),
      partitioning = arrow::schema(REGION = arrow::utf8())
    )
  } else {
    require(aws.s3)
    data_cloud <- arrow::open_dataset(
      source =   arrow::s3_bucket(
        "scoavoux",
        endpoint_override = "minio.lab.sspcloud.fr"
      )$path("records_w3/streams/streams_short"),
      partitioning = arrow::schema(REGION = arrow::utf8())
    )
  }
  query <- data_cloud %>% 
    select(is_listened, ts_listen, media_type, context_4) %>% 
    filter(media_type == "song", is_listened == 1,
           !(context_4 %in% c("ext", "other"))) %>% 
    mutate(tl = as_datetime(ts_listen),
           year = year(tl),
           wday = wday(tl, week_start = 1),
           hour = hour(tl)) %>% 
    count(wday, hour, context_4) %>% 
    group_by(wday, hour) %>% 
    mutate(f= n /sum(n))
  st <- collect(query)
  st <- st %>% 
    mutate(wk = ifelse(wday < 6, "Week", "Week-end") %>% 
             factor())  %>% 
    group_by(wk, hour, context_4) %>% 
    summarise(f = mean(f)) %>% 
    ungroup()
  theme_set(theme_minimal(base_size = 15))
  gg <- st %>% 
    ggplot(aes(hour, f, color = context_4, linetype = wk)) +
      geom_line() +
      labs(x = "Hour of day", linetype = "Time", color = "Device", y = "") +
      scale_y_continuous(limits = c(0, .85))
  filename <- "output/gg_recommendation_use_rythms.png"
  ggsave(filename, gg, width = 10)
  return(filename)
  
}
