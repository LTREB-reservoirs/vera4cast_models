generate_secchi_forecast <- function(forecast_date, # a recommended argument so you can pass the date to the function
                                     model_id,
                                     targets_url, # where are the targets you are forecasting?
                                     horizon = 30, #how many days into the future
                                     forecast_variable = c("Secchi_m_sample"),
                                     site, # what site(s)
                                     project_id = 'vera4cast') {
  
  horizon_dates <- data.frame(datetime = seq.Date(from = forecast_date + days(1), by = "day", length.out = horizon)) #if this becomes a date time we may

  #-------------------------------------
  
  # Get targets
  message('Getting targets')
  targets <- readr::read_csv(targets_url, show_col_types = F) |>
    filter(variable %in% forecast_variable,
           site_id %in% site,
           datetime < forecast_date)
  #-------------------------------------

  
  #make model
  forecasted_secchi_rolling_avg <-  targets %>%
    select(observation) %>%
    na.omit() %>%
    slice_tail(n=3) %>%
    summarise(mu = mean(observation), sigma = sd(observation)) %>%
    slice(rep(1:n(), each = 30)) %>%
    cbind(horizon_dates) %>%     #add values to dates
    pivot_longer(names_to = "parameter", values_to = "prediction", cols = c(mu, sigma))
  
  
  # Generate forecasts
  message('Generated forecast')

  forecast_df <- data.frame(datetime = horizon_dates,
                            reference_datetime = forecast_date,
                            model_id = model_id,
                            site_id = site,
                            family = 'normal',
                            variable = forecast_variable,
                            depth_m = NA,
                            duration = targets$duration[1],
                            project_id = project_id)
  
  forecast_df <- forecast_df |>
    right_join(forecasted_secchi_rolling_avg, by = c('datetime')) |>
    select(reference_datetime, datetime, site_id, model_id, variable, family, parameter, prediction, depth_m, project_id, duration)
  
  
  return(forecast_df)

}

