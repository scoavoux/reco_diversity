make_model_params <- function(model_params_file){
  model_params <- yaml::read_yaml(model_params_file)
  return(model_params)
}

fit_model <- function(user_period_div, 
                      model_params){
  require(fixest)
  div_var <- sym(model_params[[1]]$diversity[[1]])
  
  if( model_params[[1]]$log){
    user_period_div <- user_period_div %>%
      mutate({{ div_var }} := log({{ div_var }} + 1))
  }
  if( model_params[[1]]$scale){
    user_period_div <- user_period_div %>%
      mutate({{ div_var }} := scale({{ div_var }}))
  }
  if( model_params[[1]]$treatment[[1]] == "pooled") {
    char_treatment <- "c4_reco"
  } else if( model_params[[1]]$treatment[[1]] == "separate") {
    char_treatment <- "c4_reco_algo + c4_edito"
  }
  char_fe <- case_when(
     model_params[[1]]$period_fe[[1]] &  model_params[[1]]$ind_fe[[1]] ~ "| hashed_id + period",
     model_params[[1]]$period_fe[[1]] ~ "| period",
     model_params[[1]]$ind_fe[[1]] ~ "| hashed_id",
    TRUE ~ ""
  )
  char_div <-  model_params[[1]]$diversity[[1]]
  to_fit <- str_glue("{char_div} ~ {char_treatment} + log(total_play_l) {char_fe}") %>% 
    str_trim()
  res <- feols(as.formula(to_fit), data = user_period_div)
  return(res)
}

extract_treatment_effect <- function(model){
  require(fixest)
  sm <- summary(model, vcov = "twoway")
  co <- coef(sm)
  se <- se(sm)
  co <- co[str_detect(names(co), "c4")]
  se <- se[str_detect(names(se), "c4")]
  
  result <- tibble(dependant = as.character(sm$fml)[2],
                   treatment = names(co),
                   treatment_effect = co,
                   treatment_effect_se = se)
  return(result)
}

fit_model_extract_treatment_effect <- function(user_period_div, 
                                               model_params){
  require(fixest)
  div_var <- sym(model_params[[1]]$diversity[[1]])
  
  if( model_params[[1]]$log){
    user_period_div <- user_period_div %>%
      mutate({{ div_var }} := log({{ div_var }} + 1))
  }
  if( model_params[[1]]$scale){
    user_period_div <- user_period_div %>%
      mutate({{ div_var }} := scale({{ div_var }}))
  }
  if( model_params[[1]]$treatment[[1]] == "pooled") {
    char_treatment <- "c4_reco"
  } else if( model_params[[1]]$treatment[[1]] == "separate") {
    char_treatment <- "c4_reco_algo + c4_edito"
  }
  char_fe <- case_when(
    model_params[[1]]$period_fe[[1]] &  model_params[[1]]$ind_fe[[1]] ~ "| hashed_id + period",
    model_params[[1]]$period_fe[[1]] ~ "| period",
    model_params[[1]]$ind_fe[[1]] ~ "| hashed_id",
    TRUE ~ ""
  )
  char_div <-  model_params[[1]]$diversity[[1]]
  to_fit <- str_glue("{char_div} ~ {char_treatment} + log(total_play_l) {char_fe}") %>% 
    str_trim()
  model <- feols(as.formula(to_fit), data = user_period_div)
  sm <- summary(model, vcov = "twoway")
  co <- coef(sm)
  se <- se(sm)
  co <- co[str_detect(names(co), "c4")]
  se <- se[str_detect(names(se), "c4")]
  
  result <- tibble(dependant = as.character(sm$fml)[2],
                   treatment = names(co),
                   treatment_effect = co,
                   treatment_effect_se = se)
  return(result)
  
}

plot_treatment_effect <- function(models_coefs, 
                                  model_params, 
                                  what = c("popularity", "general", "omnivore", "legitimacy", "acoustic", "all")){
  theme_set(theme_minimal(base_size = 15))
  
  model_params <- bind_rows(model_params) %>%
    select(dependant = "diversity", inverted, log) %>% 
    distinct()
  
  # Do we want to invert coefficients? Usually, positive means more diversity
  # but not for some metrics (share of superstar: positive coef means lower 
  # diversity). We might want to invert the coefficient in that case, so that
  # all dots on the plot can easily be read on a negative to positive side
  # However, it might lead to counterintuitive results: for instance all coefs
  # for popularity are on the same side but it means some are positive and some
  # negative (+ superstar => - long tail). Therefore, we remove this for now.
  # models_coefs <- models_coefs %>%
  #   left_join(model_params) %>% 
  #   mutate(treatment_effect = ifelse(inverted, -1 * treatment_effect, treatment_effect))
  models_coefs <- models_coefs %>%
    left_join(model_params)
  
  what <- what[1]
  dependant_groups <- read_csv("data/dependant_groups.csv")
  models_coefs <- models_coefs %>% 
    left_join(dependant_groups)
  if(what != "all"){
    models_coefs <- models_coefs %>% 
      filter(type == what)
  }
  models_coefs <- models_coefs %>% 
    mutate(dependant = recode_vars(dependant, "cleandiversity") %>% 
             str_replace_all("\\\\n", "\n"),
           # add sign if inverted; if logged
#           dependant = ifelse(inverted, paste0(dependant, "*"), dependant),
           dependant = ifelse(log, paste0(dependant, "§"), dependant),
           dependant = factor(dependant, levels = unique(dependant)),
           treatment = recode_vars(treatment, "cleanreco") %>% 
             factor(levels = c("All", "Algorithmic", "Editorial")),
           type = factor(type, 
                         levels = c("demographics", "popularity", "acoustic", "legitimacy", "omnivore"),
                         labels = c("Artist demographics", "Popularity",
                                    "Aesthetic features", 
                                    "Cultural hierarchies", "Variance in cultural hierarchies"))
           )
  models_coefs %>% filter(str_detect(dependant, "Share of"))
  filename <- str_glue("output/gg_treatment_effect_{what}.pdf")
  gg <- ggplot(models_coefs, aes(y = dependant,
                           x = treatment_effect,
                           xmin = treatment_effect - 2*treatment_effect_se,
                           xmax = treatment_effect + 2*treatment_effect_se,
                           shape = treatment,
                           color = treatment)) +
    geom_point(position = position_dodge(width = .5)) + 
    geom_linerange(position = position_dodge(width = .5)) +
    geom_vline(xintercept = 0) +
    scale_color_brewer(palette = "Dark2") +
    labs(x = "Effect of recommendation\n(standardized)", 
         y = "",
         shape = "",
         color = "") +
    theme(legend.position = "bottom")
  if(what == "all"){
    gg <- gg + 
      facet_wrap(~ type, ncol = 2, scales='free_y')
    ggsave(filename, gg, width=40, height=25, units = "cm")
  } else {
    ggsave(filename, gg, width=19, height=12, units = "cm")
  }
  
  return(filename)
}

