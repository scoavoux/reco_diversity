fit_model <- function(users, .diversity, .treatment = "pooled", .log = FALSE, .scale = FALSE){
  require(fixest)
  if(.log){
    users <- users %>% 
      mutate({{ .diversity }} := log({{ .diversity }}))
  }
  if(.scale){
    users <- users %>% 
      mutate({{ .diversity }} := scale({{ .diversity }}))
  }
  if(.treatment == "pooled") {
    char_treatment <- "c4_reco"
  } else if(.treatment == "separate") {
    char_treatment <- "c4_reco_algo + c4_edito"
  }
  char_fe <- ""
  if(period_fe | ind_fe){
    fe <- paste0()
  }
  char_div <- deparse(substitute(.diversity))
  to_fit <- str_glue("{char_div} ~ {char_treatment} + log(total_play_l) | hashed_id + period")
  feols(as.formula(to_fit), data = users)
}
