fit_model <- function(users, 
                      model_params){
  require(fixest)
  div_var <- sym(model_params[[1]]$diversity[[1]])

  if( model_params[[1]]$log){
    users <- users %>%
      mutate({{ div_var }} := log({{ div_var }} + 1))
  }
  if( model_params[[1]]$scale){
    users <- users %>%
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
  feols(as.formula(to_fit), data = users)
}
fit_model2 <- function(params, users){
  str(params[[1]])
}

make_model_params <- function(users, model_params){
  model_params <- list(
    list(diversity = "div_artist",
         log = TRUE,
         scale = TRUE,
         treatment = "pooled",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "div_artist",
         log = TRUE,
         scale = TRUE,
         treatment = "separate",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "div_genre",
         log = TRUE,
         scale = TRUE,
         treatment = "pooled",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "div_genre",
         log = TRUE,
         scale = TRUE,
         treatment = "separate",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "mean_pop",
         log = TRUE,
         scale = TRUE,
         treatment = "pooled",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "mean_pop",
         log = TRUE,
         scale = TRUE,
         treatment = "separate",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "nb_longtail",
         log = TRUE,
         scale = TRUE,
         treatment = "pooled",
         period_fe = TRUE,
         ind_fe = TRUE),
    list(diversity = "nb_longtail",
         log = TRUE,
         scale = TRUE,
         treatment = "separate",
         period_fe = TRUE,
         ind_fe = TRUE)
    
  )
  return(model_params)
}

#fit_model(users, model_params[[1]])

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
  result
}

plot_treatment_effect <- function(models_coefs){
  theme_set(theme_minimal())
  models_coefs <- models_coefs %>% 
    mutate(dependant = recode_vars(dependant, "cleandiversity"),
           treatment = recode_vars(treatment, "cleanreco") %>% 
             factor(levels = c("All", "Algorithmic", "Editorial")))
  
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
    labs(x = "Effect of recommendation", 
         y = "Diversity",
         shape = "Recommendation",
         color = "Recommendation") +
    theme(legend.position = "bottom")
  filename <- "output/gg_treatment_effect.pdf"
  ggsave(filename, gg)
  return(filename)
}

