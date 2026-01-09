# plot only a subset of variables ------  
library(tidyverse)
library(targets)
tar_source()
tar_load(models_coefs)
tar_load(model_params)
theme_set(theme_minimal(base_size = 15))

model_params <- bind_rows(model_params) %>%
  select(dependant = "diversity", inverted, log) %>% 
  distinct()

models_coefs <- models_coefs %>%
  left_join(model_params)

dependant_groups <- read_csv("data/dependant_groups.csv")
models_coefs <- models_coefs %>% 
  left_join(dependant_groups)

models_coefs <- models_coefs %>% 
  filter(dependant %in% c("div_artist", "div_genre", "div_regional", 
                          "div_linguistic", "f_women", "sc_exo_score_mean", 
                          "sc_exo_score_sd", "f_endo_longtail", 
                          "f_endo_intermediate", "f_endo_small_star", 
                          "f_endo_star", "f_endo_superstar", 
                          "related_artists_diversity", "pc1_sd", "pc2_sd", 
                          "pc3_sd"))

models_coefs <- models_coefs %>% 
  mutate(type = ifelse(dependant %in% c("div_genre", "sc_exo_score_sd", "sc_exo_score_mean"), "acoustic", type),
         dependant = recode_vars(dependant, "cleandiversity") %>% 
           str_replace_all("\\\\n", "\n"),
         # add sign if inverted; if logged
         # dependant = ifelse(inverted, paste0(dependant, "*"), dependant),
         # dependant = ifelse(log, paste0(dependant, "§"), dependant),
         dependant = factor(dependant, levels = unique(dependant)),
         
         treatment = recode_vars(treatment, "cleanreco") %>% 
           factor(levels = c("All", "Algorithmic", "Editorial")),
         type = factor(type, 
                       levels = c("demographics", "popularity", "acoustic"),
                       labels = c("Artist demographics", "Popularity",
                                  "Aesthetic features"))
  )
dep_lab <- read_csv("oneoff/dependants.csv")
models_coefs <- models_coefs %>% 
  left_join(dep_lab) %>% 
  select(-dependant) %>% 
  rename(dependant = "dependant_new")

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
gg <- gg + 
  facet_wrap(~ type, ncol = 1, scales='free_y') +
  ggh4x::force_panelsizes(rows = c(4, 5, 7))
gg
