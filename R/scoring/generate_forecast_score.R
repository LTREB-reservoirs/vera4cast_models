generate_forecast_score <- function(targets_df,
                                          forecast_df,
                                          local_directory = NULL,
                                          variable_types = "state"){
  
  
  #output_directory <- arrow::SubTreeFileSystem$create(local_directory)
  
  
  df <- forecast_df %>%
    dplyr::filter(variable_type %in% variable_types) |>
    dplyr::mutate(family = as.character(family)) |>
    score4cast::crps_logs_score(targets_df, extra_groups = c('depth')) |>
    dplyr::mutate(horizon = datetime-lubridate::as_datetime(reference_datetime)) |>
    dplyr::mutate(horizon = as.numeric(lubridate::as.duration(horizon),
                                       units = "seconds"),
                  horizon = horizon / 86400)
  
  df <- df |> dplyr::mutate(reference_date = lubridate::as_date(reference_datetime))
  
  return(df)
  #arrow::write_dataset(df, path = output_directory$path('/flare/scores/parquet/'), partitioning = c("site_id","model_id","reference_date"))
  
}
