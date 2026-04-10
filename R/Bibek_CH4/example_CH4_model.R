########################################
# CH4 REFORECAST MODEL FUNCTION
########################################

example_CH4_model <- function(forecast_date,
                              model_id,
                              horizon,
                              forecast_variable,
                              site,
                              project_id){
  
  # Prepare training data
  targets_lm <- targets_met |> 
    pivot_wider(names_from = 'variable',
                values_from = 'observation') |>
    filter(site_id == site) |>
    filter(datetime < forecast_date)
  
  # Fit model
  fit <- lm(CH4flux_umolm2s_mean ~ daily_airtemp_C + daily_wind,
            data = targets_lm)
  
  # Model uncertainty
  resid_sd <- sd(residuals(fit), na.rm = TRUE)
  
  # Future weather
  future_weather_model <- future_weather |>
    filter(datetime >= forecast_date) |>
    select(datetime,
           site_id,
           parameter,
           daily_airtemp_C,
           daily_wind)
  
  # Predictions
  future_weather_model$prediction <- predict(
    fit,
    newdata = future_weather_model
  )
  
  # Mean forecast
  CH4_mean <- tibble(
    datetime = future_weather_model$datetime,
    site_id = future_weather_model$site_id,
    parameter = as.character(future_weather_model$parameter),
    prediction = future_weather_model$prediction,
    variable = forecast_variable,
    depth_m = NA_real_
  )
  
  # SD forecast
  CH4_sd <- CH4_mean |>
    mutate(
      prediction = resid_sd,
      parameter = "sd"
    )
  
  # Combine ensemble
  CH4_lm_forecast <- bind_rows(CH4_mean, CH4_sd) |>
    mutate(parameter = ifelse(parameter == "sd", NA_real_, as.numeric(parameter)))
  
  # Convert to VERA format
  CH4_lm_forecast_standard <- CH4_lm_forecast %>%
    mutate(
      model_id = model_id,
      reference_datetime = as.POSIXct(forecast_date, tz="UTC"),
      family = 'ensemble',
      duration = 'P1D',
      depth_m = NA_real_,
      project_id = project_id
    ) %>%
    select(datetime,
           reference_datetime,
           site_id,
           duration,
           family,
           parameter,
           variable,
           prediction,
           depth_m,
           model_id,
           project_id)
  
  return(CH4_lm_forecast_standard)
  
}