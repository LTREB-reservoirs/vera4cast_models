## Using Vera4castHelpers Function
weather_stage2 <- vera4castHelpers::noaa_stage2(start_date = "2025-08-01")

df_future_hourly <- weather_stage2 |> 
  dplyr::filter(site_id == "fcre") |>
  dplyr::collect()

df_future_daily <- df_future_hourly |> 
  dplyr::mutate(date = as.Date(datetime)) |> 
  dplyr::group_by(date, variable, parameter, site_id) |> 
  dplyr::summarise(prediction = mean(prediction)) |> 
  dplyr::ungroup()

 

## Ropenmento funcitons for pulling hourly and daily met
get_hourly_weather <- function(current_site, site_list, past = 90, future = 30,
                               model = "gfs_seamless", vars = c("relativehumidity_2m",
                                                                "precipitation",
                                                                "windspeed_10m",
                                                                "cloudcover",
                                                                "temperature_2m",
                                                                "shortwave_radiation")) {
  
  #Grab the lat/long of the sites
  lat <- site_list |>
    filter(site_id == current_site) |>
    select(latitude) |>  pull()
  
  long <-  site_list |>
    filter(site_id == current_site) |>
    select(longitude) |>  pull()
  #-----------------------------
  
  site_weather <- RopenMeteo::get_ensemble_forecast(
    latitude = lat,
    longitude = long,
    forecast_days = future, # days into the future
    past_days = past, # past days that can be used for model fitting
    model = model,
    variables = vars) |>
    RopenMeteo::convert_to_efi_standard() |>
    mutate(site_id = current_site)
  #-------------------------------------
  
  return(site_weather)
}


get_daily_weather <- function(current_site, site_list, past = 90, future = 30,
                              model = "gfs_seamless", vars = c("relativehumidity_2m",
                                                               "precipitation",
                                                               "windspeed_10m",
                                                               "cloudcover",
                                                               "temperature_2m",
                                                               "shortwave_radiation")) {
  
  #Grab the lat/long of the sites
  lat <- site_list |>
    filter(site_id == current_site) |>
    select(latitude) |>  pull()
  
  long <-  site_list |>
    filter(site_id == current_site) |>
    select(longitude) |>  pull()
  #-------------------------------------
  
  # Collect th relevent weather data
  site_weather <- RopenMeteo::get_ensemble_forecast(
    latitude = lat,
    longitude = long,
    forecast_days = future, # days into the future
    past_days = past, # past days that can be used for model fitting
    model = model,
    variables = vars) |>
    
    # convert to a standardised forecast
    RopenMeteo::convert_to_efi_standard() |>
    mutate(site_id = current_site,
           datetime = as_date(datetime)) |>
    group_by(datetime, site_id, variable, parameter) |>
    
    #calcuate the daily mean
    summarise(prediction = mean(prediction))
  #-------------------------------------
  
  return(site_weather)
}