library(tidyverse)
library(arrow)
library(vera4castHelpers)

## helper functions
#function to pull current value
current_value <- function(dataframe, variable, start_date){

  value <- dataframe |>
    mutate(datetime = as.Date(datetime)) |>
    filter(datetime == start_date,
           variable == variable) |>
    pull()

  return(value)
}

#function to generate 30 ensembles of fDOM IC based on standard deviation arround current observation
# get_IC_uncert <- function(current_value, n_members, ic_sd = 0.05){
#   rnorm(n = n_members, mean = current_value, sd = ic_sd)
# }

## main function
# Model: vera_precip_ar3 — log_flow ~ prcp_3d + lag1 + lag2 + lag3
# where log_flow = log(Flow_cms_mean), prcp_3d = 3-day antecedent precip (mm)
# Precipitation forecast: NOAA GEFS stage2 precipitation_flux ensemble (kg m-2 s-1 -> mm/day)

generate_inflow_forecast <- function(forecast_date,       # Date object: the reference date for this forecast
                                     forecast_horizon,     # integer: number of days ahead (e.g. 34)
                                     n_members,            # integer: number of ensemble members (e.g. 31)
                                     model_id,             # string: model identifier for vera output
                                     inflow_targets_url,   # vera4cast daily-inflow-targets.csv.gz URL
                                     met_targets_url,      # vera4cast daily-met-targets.csv.gz URL
                                     noaa_4cast_url,       # S3 bucket path: "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"
                                     calibration_start_date = as.Date("2015-07-07"),
                                     project_id   = "vera4cast",
                                     site,
                                     var) {
  
  # forecast_date = Sys.Date() - lubridate::days(30)     # Date object: the reference date for this forecast
  # forecast_horizon = 34     # integer: number of days ahead (e.g. 34)
  # n_members = 31            # integer: number of ensemble members (e.g. 31)
  # model_id = "inflow_ar_abp"            # string: model identifier for vera output
  # inflow_targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-inflow-targets.csv.gz"  # vera4cast daily-inflow-targets.csv.gz URL
  # met_targets_url = "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-met-targets.csv.gz"      # vera4cast daily-met-targets.csv.gz URL
  # noaa_4cast_url = "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"      # S3 bucket path: "bio230121-bucket01/flare/drivers/met/gefs-v12/stage2"
  # calibration_start_date = as.Date("2015-07-07")
  # project_id   = "vera4cast"
  # var = "Flow_cms_mean"
  # 
  #FLOW_FLOOR <- 1e-4  # m3/s minimum to avoid log(0)
  
  #-------------------------------------
  # 1. Load historical inflow targets (Flow_cms_mean at inflow_site)
  #-------------------------------------
  message("Getting inflow targets")
  inflow_hist <- readr::read_csv(inflow_targets_url, show_col_types = FALSE) |>
    filter(variable == "Flow_cms_mean") |>
    filter(datetime >= calibration_start_date, 
           datetime < forecast_date) |>
    select(datetime, variable, observation) |>
    pivot_wider(names_from = variable, values_from = observation)|>
    arrange(datetime) 
   
  
  #-------------------------------------
  # 2. Load historical met precipitation targets (Rain_mm_sum at met_site)
  #-------------------------------------
  message("Getting met precipitation targets")
  precip_hist <- readr::read_csv(met_targets_url, show_col_types = FALSE) |>
    filter(variable == "Rain_mm_sum") |>
    select(datetime, variable, observation)|>
    pivot_wider(names_from = variable, values_from = observation)
  
  #-------------------------------------
  # 4. Get NOAA GEFS precipitation forecast (stage2, site=fcre)
  #    precipitation_flux is in kg m-2 s-1; convert to mm/day (* 86400)
  #-------------------------------------
  message("Getting NOAA GEFS precipitation forecast")
  
  # Get the NOAA precp forecast and get the sum of the last three days for each ensemble_member
  
  noaa_date <- forecast_date    # GEFS issued day before
  met_site = "fcre"
  
  met_s3 <- arrow::s3_bucket(
    paste0(noaa_4cast_url, "/reference_datetime=", noaa_date, "/site_id=", met_site),
    endpoint_override = "amnh1.osn.mghpcc.org",
    anonymous = TRUE
  )
  
  gefs_precip <- arrow::open_dataset(met_s3) |>
    filter(variable == "precipitation_flux") |>
    select(datetime, parameter, prediction) |>
    collect() |>
    mutate(
      date    = as.Date(datetime),
      prcp_mm = prediction * 86400,          # kg m-2 s-1 -> mm/day
      ensemble_member = as.integer(parameter) +1
    ) |>
    group_by(date, ensemble_member)|>
    summarise(forecast_rain_mm_sum = sum(prcp_mm)) |>
    ungroup() |>
    filter(date >= forecast_date,
           date < forecast_date + lubridate::days(forecast_horizon + 1),
           ensemble_member <= n_members) |>
    select(date, ensemble_member, forecast_rain_mm_sum) |>
    arrange(ensemble_member, date) |>
    dplyr::rename(datetime = date) 
    
  
  if (nrow(gefs_precip) == 0) {
    stop("No GEFS precipitation data found for forecast_date = ", forecast_date,
         " — check noaa_4cast_url and met_site")
  }
  
  #  # Get the NOAA precp forecast and get the sum of the last three days for each ensemble_member
  
  noaa_date2 <- forecast_date - lubridate::days(3)
  
  met_s3_2 <- arrow::s3_bucket(
    paste0(noaa_4cast_url, "/reference_datetime=", noaa_date - lubridate::days(3), "/site_id=", met_site),
    endpoint_override = "amnh1.osn.mghpcc.org",
    anonymous = TRUE
  )
  
  gefs_precip_2 <- arrow::open_dataset(met_s3_2) |>
    filter(variable == "precipitation_flux") |>
    select(datetime, parameter, prediction) |>
    collect() |>
    mutate(
      date    = as.Date(datetime),
      prcp_mm = prediction * 86400,          # kg m-2 s-1 -> mm/day
      ensemble_member = as.integer(parameter) +1
    ) |>
    group_by(date, ensemble_member)|>
    summarise(forecast_rain_mm_sum = sum(prcp_mm)) |>
    ungroup() |>
    filter(date >= forecast_date - lubridate::days(3),
           date < forecast_date,
           ensemble_member <= n_members) |>
    select(date, ensemble_member, forecast_rain_mm_sum) |>
    arrange(ensemble_member, date) |>
    dplyr::rename(datetime = date) 
    # group_by(ensemble_member) |>
    # mutate(
    #   prcp_lag1 = lag(forecast_rain_mm_sum, 1),
    #   prcp_lag2 = lag(forecast_rain_mm_sum, 2),
    #   prcp_lag3 = lag(forecast_rain_mm_sum, 3)) |>
    # ungroup()
  
  # add the two data frames together and then calculate the last three days precip
  
  forecast_precp <- bind_rows(gefs_precip_2, gefs_precip)|>
    arrange(ensemble_member, datetime)|>
    group_by(ensemble_member) |>
    mutate(
      prcp_lag1 = lag(forecast_rain_mm_sum, 1),
      prcp_lag2 = lag(forecast_rain_mm_sum, 2),
      prcp_lag3 = lag(forecast_rain_mm_sum, 3)) |>
  ungroup()|>
  group_by(datetime, ensemble_member)|>
  summarise(NOAA_prcp_3d = prcp_lag1) |> #+ prcp_lag2)) |> # + prcp_lag3)) |>
  ungroup() |>
  filter(datetime >= forecast_date)|>
  mutate(
    reference_datetime = forecast_date
  )

  #-------------------------------------
  # 3. Build calibration data frame and fit vera_precip_ar3 model
  #    log_flow ~ prcp_3d + lag1 + lag2 + lag3
  #    prcp_3d = sum of precip on days t-1, t-2, t-3 (no same-day leakage)
  #-------------------------------------
  message("Fitting vera_precip_ar3 model")
  
  fit_df <- inflow_hist |>
    left_join(precip_hist, by = "datetime") |>
    drop_na()|>
    arrange(datetime)|> 
    mutate(
      prcp_lag1 = lag(Rain_mm_sum, 1),
      prcp_lag2 = lag(Rain_mm_sum, 2),
      prcp_lag3 = lag(Rain_mm_sum, 3),
      prcp_3d   = prcp_lag1, # + prcp_lag2 #+ prcp_lag3,
      lag1_flow      = lag(Flow_cms_mean, 1),
      lag2_flow      = lag(Flow_cms_mean, 2),
      lag3_flow     = lag(Flow_cms_mean, 3)
    ) |>
    filter(!is.na(prcp_3d), !is.na(lag3_flow))
  
  flow_model <- lm(Flow_cms_mean ~ prcp_3d + lag1_flow + lag2_flow + lag3_flow, data = fit_df)
  #flow_model <- lm(Flow_cms_mean ~ prcp_3d + lag1_flow, data = fit_df)
  coeffs    <- coef(flow_model)
  sigma     <- sd(residuals(flow_model), na.rm = TRUE)
  
  message("Model fit summary:")
  print(summary(flow_model))


# figure out how to get standard error from the lm 
coeffs <- flow_model$coefficients
params_se <- coef(summary(flow_model))[, "Std. Error"]  

# #### get param uncertainty
## get param distribtuions for parameter uncertainity. Not using parameter uncertainity right now.
param_df <- data.frame(beta_int = rnorm(n_members, coeffs[1], params_se[1]),
                       beta_prcp_3d = rnorm(n_members, coeffs[2], params_se[2]),
                       beta_flow_lag1 = rnorm(n_members, coeffs[3], params_se[3]),
                       beta_flow_lag2 = rnorm(n_members, coeffs[4], params_se[4]),
                       beta_flow_lag3 = rnorm(n_members, coeffs[5], params_se[5]))



####get process uncertainty
#find residuals. Not using the process uncertainty right now
fit_df_noNA <- na.omit(fit_df) # take out any NAs from the targets file
fit_df_noNA$mod <- predict(flow_model, data = fit_df_noNA)
residuals <- fit_df_noNA$mod - fit_df_noNA$Flow_cms_mean
sigma <- sd(residuals, na.rm = TRUE) # Process Uncertainty Noise Std Dev.; this is your sigma


####look at set up for IC uncert  # come back to this
ic_sd <- 0.002 #adpating from HLWs temp 4cast using 0.1 and detection limit on fDOM sensor being 0.07 QSU, 2 uS/cm is the accuracy from the EXO manual
ic_uc <- rnorm(n = n_members, mean = mean(inflow_hist %in% var, na.rm = T), sd = ic_sd)
hist(ic_uc)

param_df$sigma <- sigma
param_df$ic_sd <- ic_sd

#return(param_df)

#-------------------------------------





#-------------------------------------

# Set up forecast data frame

message('Make forecast dataframe')

forecast_date_adjust <- forecast_date 

#establish forecasted dates
forecasted_dates <- seq(from = ymd(forecast_date_adjust), to = ymd(forecast_date_adjust) + forecast_horizon, by = "day")

#get current spcond value
#curr_flow <- current_value(dataframe = inflow_hist,variable = var, start_date = forecast_date_adjust)
curr_flow_1 <- current_value(dataframe = inflow_hist,variable = var, start_date = forecast_date - lubridate::days(1))
curr_flow_2 <- current_value(dataframe = inflow_hist,variable = var, start_date = forecast_date - lubridate::days(2))
curr_flow_3 <- current_value(dataframe = inflow_hist,variable = var, start_date = forecast_date - lubridate::days(3))

# #set up df of different initial conditions for IC uncert
# ic_df <- tibble(date = rep(as.Date(forecast_date), times = n_members),
#                 ensemble_member = c(1:n_members),
#                 forecast_variable = var,
#                 value = rnorm(n = n_members, mean = curr_flow, sd = 0.1),
#                 uc_type = "initial")

##set up df of different initial conditions for IC uncert
ic_df_1 <- tibble(date = rep(as.Date(forecast_date)- lubridate::days(1), times = n_members),
                  ensemble_member = c(1:n_members),
                  reference_datetime = forecast_date,
                  Horizon = date - reference_datetime, 
                  forecast_variable = var,
                  value = rnorm(n = n_members, mean = curr_flow_1, sd = ic_sd),
                  uc_type = "initial")

##set up df of different initial conditions for IC uncert
ic_df_2 <- tibble(date = rep(as.Date(forecast_date) - lubridate::days(2), times = n_members),
                  ensemble_member = c(1:n_members),
                  reference_datetime = forecast_date,
                  Horizon = date - reference_datetime,
                  forecast_variable = var,
                  value = rnorm(n = n_members, mean = curr_flow_2, sd = ic_sd),
                  uc_type = "initial")
#set up df of different initial conditions for IC uncert
ic_df_3 <- tibble(date = rep(as.Date(forecast_date) - lubridate::days(3), times = n_members),
                  ensemble_member = c(1:n_members),
                  reference_datetime = forecast_date,
                  Horizon = date - reference_datetime,
                  forecast_variable = var,
                  value = rnorm(n = n_members, mean = curr_flow_3, sd = ic_sd),
                  uc_type = "initial")

#set up table to hold forecast output 
forecast_full_unc <- tibble(date = rep(forecasted_dates, times = n_members),
                            ensemble_member = rep(1:n_members, each = length(forecasted_dates)),
                            reference_datetime = forecast_date,
                            Horizon = date - reference_datetime,
                            forecast_variable = var,
                            value = as.double(NA),
                            uc_type = "initial")|>
  bind_rows(ic_df_1)|>
  bind_rows(ic_df_2)|>
  bind_rows(ic_df_3)



#-------------------------------------

### ABP issue. Why can't I get forecast output values. Need to figure out how to get multiple days lagged for input. 
# My initial conditional uncertainty is wayyy to big 

message('Generating forecast')

# add to the forecast v
#establish forecasted dates
forecasted_dates <- seq(from = ymd(forecast_date_adjust) - lubridate::days(3), to = ymd(forecast_date_adjust) + forecast_horizon, by = "day")

print(paste0('Running forecast starting on: ', forecast_date))

#for loop to run forecast 
for(i in 4:length(forecasted_dates)) {
  
  #pull prediction dataframe for the relevant date
  flow_pred <- forecast_full_unc %>%
    filter(date == forecasted_dates[i])
  
  # pull in the forecasted precip data that is already in the format of the last three day observation for each ensemble member. 
  precp_drive <- forecast_precp %>%
    filter(as.Date(reference_datetime) == forecast_date) |> 
    filter(as.Date(datetime) == forecasted_dates[i]) |>
    slice(1:31)
  
  #pull lagged 1 spcond values
  flow_lag <- forecast_full_unc %>%
    filter(date == forecasted_dates[i-1])
  
  #pull lagged 2 spcond values
  flow_lag_2 <- forecast_full_unc %>%
    filter(date == forecasted_dates[i-2])

  #pull lagged 3 spcond values
  flow_lag_3 <- forecast_full_unc %>%
    filter(date == forecasted_dates[i-3])
  
  #run model. # parameter uncertainty very high! Just using the ensemble from the water residence time for the uncertainity 
  # flow_pred$value <- coeffs[1] + (precp_drive$NOAA_prcp_3d * coeffs[2])  +
  #   (flow_lag$value * coeffs[3]) + (flow_lag_2$value * coeffs[4]) +
  #   (flow_lag_3$value * coeffs[5]) +
  #   rnorm(n = 31, mean = 0, sd = sigma) #process uncert

  flow_pred$value <- param_df$beta_int + (precp_drive$NOAA_prcp_3d * param_df$beta_prcp_3d)  +
    (flow_lag$value * param_df$beta_flow_lag1) + (flow_lag_2$value * param_df$beta_flow_lag2) +
    (flow_lag_3$value * param_df$beta_flow_lag3) +
    rnorm(n = 31, mean = 0, sd = sigma) #process uncert
  
  
  #insert values back into the forecast dataframe
  forecast_full_unc <- forecast_full_unc %>%
    rows_update(flow_pred, by = c("date","ensemble_member","forecast_variable","uc_type"))
  
} #end for loop

#clean up file to match vera format 

forecast_df <- forecast_full_unc |>
  rename(datetime = date,
         variable = forecast_variable,
         prediction = value,
         parameter = ensemble_member) |>
  mutate(family = 'ensemble',
         duration = "P1D",
         depth_m = NA,
         project_id = project_id,
         model_id = model_id,
         site_id = site,
         prediction = ifelse(prediction<0, 0.002, prediction)
  ) |>
  filter(datetime >= forecast_date) |>
  select(datetime, reference_datetime, model_id, site_id,
         parameter, family, prediction, variable, depth_m,
         duration, project_id)

return(forecast_df)
#return(write.csv(forecast_df, file = output_folder, row.names = F))
# return(write.csv(forecast_df, file = paste0("C:/Users/dwh18/OneDrive/Desktop/R_Projects/fDOM_forecasting/Data/ASLO_talk_forecast_output/", output_folder, "/forecast_full_unc_", forecast_date, '.csv'), row.names = F))


}  ##### end function
