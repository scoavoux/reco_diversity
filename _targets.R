# Preparation ------
library(targets)
library(tarchetypes)


tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  repository = "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "reco_diversity"
    )
  )
)

tar_source("R")

# List of targets ------
list(
  ## Prepare streaming data ------
  tar_target(streaming_data_files,                  list_streaming_data_files()),
  tar_file(artists_to_remove_file,                  "data/artists_to_remove.csv"),
  tar_file(artists_pop_file,                        "data/artists_pop.csv"),
  tar_target(artists_pop,                           make_artists_pop(artists_pop_file)),
  tar_target(artists_to_remove,                     make_artists_to_remove(artists_to_remove_file)),
  tar_target(items,                                 make_items_data()),
  tar_target(genres,                                make_genre_data()),
  tar_target(user_artist_per_period_onefile,        make_user_artist_per_period_table_onefile(streaming_data_files, 
                                                                                              items,
                                                                                              interval = "week"), 
                                                    pattern = streaming_data_files),
  tar_target(user_artist_per_period,                merge_user_artist_per_period_table(user_artist_per_period_onefile, artists_to_remove = artists_to_remove)),
  # tar_target(user_genre_summary_data_prop,       make_user_genre_summary_data(user_artist_per_period_merged_artists, genres, proportion=TRUE)),
  # tar_target(user_genre_summary_data_raw ,       make_user_genre_summary_data(user_artist_per_period_merged_artists, genres, proportion=FALSE)),
  
  tar_target(unique_artists,      make_unique_artists(user_artist_per_period)),
  tar_target(unique_artists_csv,  export_unique_artists(unique_artists), 
             format = "file",
             repository = "local"),
  tar_target(gender,              make_artists_gender(unique_artists)),
  
  ## Prepare user data ------
  tar_target(user_reco,           compute_use_of_recommendations(user_artist_per_period)),
  tar_target(user_artist_div,     compute_artist_diversity(user_artist_per_period)),
  tar_target(user_genre_div,      compute_genre_diversity(user_artist_per_period, genres)),
  tar_target(user_pop_div,        compute_pop_diversity(user_artist_per_period, artists_pop)),
  tar_target(user_gender_div,     compute_gender_diversity(user_artist_per_period, gender)),
  tar_target(users,               make_user_period_level_data(user_reco,
                                                              user_artist_div,
                                                              user_genre_div,
                                                              user_pop_div,
                                                              user_gender_div)),
  
  ## Descriptive stats ------
  
  ## Run main analysis ------
  tar_target(model_params,      make_model_params()),
  tar_target(models_fit,        fit_model(users, model_params),
                                pattern = model_params),
  tar_target(models_coefs,      extract_treatment_effect(models_fit),
                                pattern = models_fit),

  ## Output ------
  tar_target(gg_treatment_effect,  plot_treatment_effect(models_coefs), 
             format = "file",
             repository = "local")
  )
