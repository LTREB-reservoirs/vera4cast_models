example_CH4_model <- function(forecast_date, model_id, horizon,
                              forecast_variable, site, project_id) {
  
  targets_lm <- targets_combined |>
    pivot_wider(names_from  = 'variable',
                values_from = 'observation') |>
    filter(site_id  == site,
           datetime <  as.POSIXct(forecast_date, tz = "UTC"))
  
  fit_formula <- as.formula(
    paste(forecast_variable, "~ AirTemp_C_mean + WindSpeed_ms_mean")
  )
  fit      <- lm(fit_formula, data = targets_lm)
  resid_sd <- sd(residuals(fit), na.rm = TRUE)
  
  future_weather_model <- future_weather |>
    filter(datetime >= as.POSIXct(forecast_date, tz = "UTC")) |>
    mutate(AirTemp_C_mean    = AirTemp_C_mean,
           WindSpeed_ms_mean = WindSpeed_ms_mean)
  
  # Option 1 — propagate uncertainty per ensemble member
  future_weather_model$prediction <- predict(fit, newdata = future_weather_model)
  
  # Add random noise per ensemble member per day (scaled by resid_sd)
  set.seed(42)
  future_weather_model <- future_weather_model |>
    group_by(parameter) |>
    mutate(
      prediction = prediction + rnorm(n(), mean = 0, sd = resid_sd)
    ) |>
    ungroup()
  
  CH4_mean <- tibble(
    datetime   = future_weather_model$datetime,
    site_id    = future_weather_model$site_id,
    parameter  = as.character(future_weather_model$parameter),
    prediction = future_weather_model$prediction,
    variable   = forecast_variable,
    depth_m    = NA
  )
  
  CH4_sd <- CH4_mean |>
    mutate(prediction = resid_sd,
           parameter  = "sd")
  
  CH4_lm_forecast <- bind_rows(CH4_mean, CH4_sd) |>
    mutate(parameter = ifelse(parameter == "sd", NA, as.numeric(parameter)))
  
  CH4_lm_forecast_standard <- CH4_lm_forecast |>
    mutate(
      model_id           = model_id,
      reference_datetime = as.POSIXct(forecast_date, tz = "UTC"),
      family             = 'ensemble',
      duration           = 'P1D',
      depth_m            = NA,
      project_id         = project_id
    ) |>
    select(datetime, reference_datetime, site_id, duration, family,
           parameter, variable, prediction, depth_m, model_id, project_id)
  
  return(CH4_lm_forecast_standard)
}