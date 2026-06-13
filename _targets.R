# Preparation ------
library(targets)
library(tarchetypes)

# Single switch (see R/common_functions.R::use_synthetic_data):
# RECO_DIVERSITY_DATA=synthetic runs the whole pipeline offline against the
# local synthetic data set, storing intermediate targets locally instead of on
# S3. Default (unset/"real") keeps the original S3-backed behaviour.
.use_synthetic <- tolower(Sys.getenv("RECO_DIVERSITY_DATA", "real")) == "synthetic"

tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  format = "feather",
  repository = if(.use_synthetic) "local" else "aws",
  repository_meta = if(.use_synthetic) "local" else "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "reco_diversity"
    )
  )
)

tar_source("R")

# Analysis parameters ------
# Sample-restriction thresholds used when assembling the user x week panel and
# the Bartik baseline. The defaults (real-data values) are the FIRST column; the
# synthetic data set is much smaller, so the offline run uses the lower values.
# These are passed explicitly into the targets below (no hidden magic numbers).
analysis_params <- if (.use_synthetic) {
  list(min_users_per_period      = 50L,    # >= this many users per period
       min_hours_played          = 0.5,    # min hours played per user-week
       baseline_week_threshold   = 8L,     # min 2019 weeks for a Bartik baseline
       baseline_volume_threshold = 10)     # min 2019 hours for a Bartik baseline
} else {
  list(min_users_per_period      = 1000L,
       min_hours_played          = 2,
       baseline_week_threshold   = 20L,
       baseline_volume_threshold = 100)
}

# List of targets ------
list(
  ## Declares files
  
  # todo
  tar_target(artists_to_remove_file,                "data/artists_to_remove.csv",
             format = "file",
             repository = "local"),
  tar_target(model_params_file,                     "data/model_params.yaml", 
             format = "file", 
             repository = "local"),
  tar_target(streaming_data_files,                  list_streaming_data_files(),
             format = "qs"),
  
  ## Prepare streaming data ------
  tar_target(users,                                 make_user_data()),
  tar_target(artists_to_remove,                     make_artists_to_remove(artists_to_remove_file)),
  tar_target(items,                                 make_items_data()),
  tar_target(genres,                                make_genre_data()),
  tar_target(acoustic_features,                     make_items_acoustic_features_data(items)),
  tar_target(acoustic_features_pca,                 make_acoustic_features_pca(acoustic_features), format = "qs"),
  tar_target(acoustic_features_pca_data,            make_acoustic_features_pca_data(acoustic_features_pca, acoustic_features)),
  tar_target(acoustic_features_with_pca,            full_join(acoustic_features, acoustic_features_pca_data)),
  tar_target(user_song_per_period_onefile,          make_user_song_per_period_onefile(streaming_data_files,
                                                                                      users,
                                                                                      interval = "week"), 
                                                    pattern = streaming_data_files),
  tar_target(user_song_per_period,                  merge_user_song_per_period(user_song_per_period_onefile)),
  tar_target(user_artist_per_period,                make_user_artist_per_period(user_song_per_period, items, artists_to_remove)),
  tar_target(user_context4_onefile,                 make_user_context4_onefile(streaming_data_files),
                                                    pattern = streaming_data_files),
  # tar_target(user_genre_summary_data_prop,        make_user_genre_summary_data(user_artist_per_period_merged_artists, genres, proportion=TRUE)),
  # tar_target(user_genre_summary_data_raw ,        make_user_genre_summary_data(user_artist_per_period_merged_artists, genres, proportion=FALSE)),
  
  tar_target(unique_artists,      make_unique_artists(user_artist_per_period)),
  tar_target(unique_artists_csv,  export_unique_artists(unique_artists), 
             format = "file",
             repository = "local"),
  tar_target(artists_pop,         make_artists_pop()),
  tar_target(gender,              make_artists_gender(unique_artists)),
  tar_target(area,                make_artists_area(unique_artists)),
  tar_target(language,            make_artists_language(unique_artists)),
  tar_target(release,             make_artists_release(unique_artists)),
  tar_target(artist_cluster,      make_artists_cluster()),
  
  ## Prepare user data ------
  tar_target(user_reco,           compute_use_of_recommendations(user_artist_per_period)),
  tar_target(user_acoustic_div,   compute_acoustic_diversity(user_song_per_period, acoustic_features_with_pca)),
  tar_target(user_artist_div,     compute_artist_diversity(user_artist_per_period)),
  tar_target(user_genre_div,      compute_genre_diversity(user_artist_per_period, genres)),
  # removed pop div: endogenous (measures fan at the end of the period)
  #tar_target(user_pop_div,        compute_pop_diversity(user_artist_per_period, artists_pop)),
  tar_target(user_endopop_div,    compute_endo_pop_diversity(user_artist_per_period)),
  tar_target(user_gender_div,     compute_gender_diversity(user_artist_per_period, gender)),
  tar_target(user_regional_div,   compute_regional_diversity(user_artist_per_period, area)),
  tar_target(user_linguistic_div, compute_linguistic_diversity(user_artist_per_period, language)),
  tar_target(user_omnivore_div,   compute_legitimacy_diversity(user_artist_per_period, artist_legitimacy)),
  tar_target(user_release_recency,compute_release_recency(user_artist_per_period, release)),
  tar_target(user_related_art_div, compute_related_artists_diversity(user_artist_per_period, artist_cluster)),
  tar_target(user_instrument,        make_recoshare_instrument(user_reco,
                                                               week_threshold   = analysis_params$baseline_week_threshold,
                                                               volume_threshold = analysis_params$baseline_volume_threshold)),
  ## Put everything together
  tar_target(user_period_div,     make_user_period_level_data(user_reco,
                                                              user_artist_div,
                                                              user_genre_div,
                                                              #user_pop_div,
                                                              user_endopop_div,
                                                              user_regional_div,
                                                              user_linguistic_div,
                                                              user_acoustic_div,
                                                              user_gender_div,
                                                              user_omnivore_div,
                                                              user_release_recency,
                                                              user_related_art_div,
                                                              user_instrument,
                                                              min_users_per_period = analysis_params$min_users_per_period,
                                                              min_hours_played     = analysis_params$min_hours_played)),
  
  ## Descriptive stats ------
  
  ## Run main analysis ------
  tar_target(model_params,      make_model_params(model_params_file),
             format = "qs"),
  # tar_target(models_fit,        fit_model(user_period_div, model_params),
  #                               pattern = model_params,
  #            format = "qs"),
  # tar_target(models_coefs,      extract_treatment_effect(models_fit),
  #                               pattern = models_fit,
  #            format = "qs"),
  
  # don't save intermediary results, too big
  tar_target(models_coefs,      fit_model_extract_treatment_effect(user_period_div, model_params),
                                pattern = model_params,
             format = "qs"),

  tar_target(iv_models_coefs, fit_bartik_model_extract_treatment_effect(user_period_div, model_params),
             pattern = model_params,
             format = "qs"),
  
  ## Main output ------
  tar_target(gg_treatment_effect_general,  plot_treatment_effect(models_coefs, model_params, what = "demographics"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_omnivore,  plot_treatment_effect(models_coefs, model_params, what = "omnivore"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_legitimacy,  plot_treatment_effect(models_coefs, model_params, what = "legitimacy"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_acoustic,  plot_treatment_effect(models_coefs, model_params, what = "acoustic"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_popularity,  plot_treatment_effect(models_coefs, model_params, what = "popularity"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_all,  plot_treatment_effect(models_coefs, model_params, what = "all"), 
             format = "file",
             repository = "local"),

  ## Bartik IV model ------
  tar_target(gg_treatment_effect_general_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "demographics", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_omnivore_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "omnivore", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_legitimacy_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "legitimacy", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_acoustic_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "acoustic", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_popularity_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "popularity", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  tar_target(gg_treatment_effect_all_bartik,  plot_treatment_effect(iv_models_coefs, model_params, what = "all", postfix="_bartik"), 
             format = "file",
             repository = "local"),
  
    
  ## Supplementary analyses ------
  tar_target(descriptive_stats,                 compute_descriptive_stats(user_artist_per_period),
             format = "file",
             repository = "local"),
  tar_target(gg_dependant_density_raw,          plot_dependant_variables_density(user_period_div, .transformation = "raw"),
             format = "file",
             repository = "local"),
  tar_target(gg_dependant_density_transformed,  plot_dependant_variables_density(user_period_div, .transformation = "transformed"),
             format = "file",
             repository = "local"),
  tar_target(gg_change_recommendation_use,      plot_recommendation_use_change(user_period_div),
             format = "file",
             repository = "local"),
  tar_target(gg_context_ternary,                plot_context_ternary(user_period_div),
             format = "file",
             repository = "local"),
  tar_target(gg_recommendation_use_by_year,     plot_recommendation_use_by_year(user_period_div),
             format = "file",
             repository = "local"),
  tar_target(gg_algorithms_use_by_genre_year,   plot_algorithms_use_by_genre_year(user_artist_per_period, genres, .by_year = TRUE),
             format = "file",
             repository = "local"),
  tar_target(gg_algorithms_use_by_genre_global, plot_algorithms_use_by_genre_year(user_artist_per_period, genres, .by_year = FALSE),
             format = "file",
             repository = "local"),
  tar_target(gg_recommendation_use_rythms,      plot_recommendation_use_rythms(),
             format = "file",
             repository = "local")

  )
