### Forecast nutrients
library(tidyverse)

inflow_targets_file <- 
  
  met_target_file <- "https://s3.flare-forecast.org/targets/fcre_v2/fcre/observed-met_fcre.csv"

horizon <- 35
reference_datetime <- Sys.Date()  
ensemble_members <- 31

inflow_targets <- t

inflow_hist_dates <- tibble(datetime = seq(min(inflow_targets$datetime), max(inflow_targets$datetime), by = "1 day"))

filled_targets_long <- inflow_targets |> 
  filter(!variable %in% c("DN_mgL_sample", "DC_mgL_sample")) |> 
  select(datetime, variable, observation) |> 
  pivot_wider(names_from = variable, values_from = observation) |> 
  right_join(inflow_hist_dates, by = "datetime") |> 
  mutate(across(flow_cms_mean:DIC_mgL_sample, imputeTS::na_interpolation)) |> 
  tidyr::fill(flow_cms_mean:DIC_mgL_sample, .direction = "up") |>
  tidyr::fill(flow_cms_mean:DIC_mgL_sample, .direction = "down") |> 
  pivot_longer(-datetime, names_to = "variable", values_to = "observation")

filled_targets_long |> 
  ggplot(aes(x = datetime, y = observation)) +
  geom_line() +
  facet_wrap(~variable, scale = "free")

forecast_datetimes <- seq(reference_datetime, length.out = horizon, by = "1 day")

variables <- filled_targets_long |> 
  filter(!variable %in% c("flow_cms_mean","temp_c_mean")) |> 
  distinct(variable) |> 
  pull(variable)

doy <- filled_targets_long |> 
  mutate(observation = ifelse(variable == "temp_c_mean", observation + 273.15, observation), 
         observation = ifelse(observation == 0, 0.0001, observation),
         doy = lubridate::yday(datetime),
         log_observation = log(observation)) |> 
  summarize(mean = mean(log_observation),
            sd = 0.1,
            .by = c("doy", "variable"))

forecast_nutrient_df <- NULL

for(i in 1:length(forecast_datetimes)){
  for(k in 1:length(variables)){
    
    curr <- doy |> filter(doy == lubridate::yday(forecast_datetimes[i]),
                          variable == variables[k])
    
    samples <- rnorm(ensemble_members, mean = curr$mean, sd = curr$sd)
    samples <- exp(samples)
    current_df <- tibble(datetime = forecast_datetimes[i],
                         variable = variables[k],
                         prediction = samples,
                         parameter = 1:ensemble_members)
    
    forecast_nutrient_df <- bind_rows(forecast_nutrient_df, current_df)
  }
}

forecast_nutrient_df <- forecast_nutrient_df |> 
  mutate(prediction = ifelse(variable == "temp_c_mean", prediction - 273.15, prediction)) 

# Fore 
df_met <- readr::read_csv(met_target_file, show_col_types = FALSE)

df_met_precip <- df_met |> 
  filter(variable == "precipitation_flux" ) |> 
  mutate(observation = observation * 60 * 24) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  group_by(date) |> 
  summarise(precip = sum(observation, na.rm = TRUE)) |> 
  mutate(lag = lag(precip),
         tenday = zoo::rollsum(precip, k = 10, align = "right", fill = NA),
         fiveday = zoo::rollsum(precip, k = 5, align = "right", fill = NA),
         twentyday = zoo::rollsum(precip, k = 20, align = "right", fill = NA)) |> 
  rename(datetime = date)

df <- RopenMeteo::get_ensemble_forecast(
  latitude = 37.30,
  longitude = -79.83,
  forecast_days = horizon,
  past_days = 20,
  model = "gfs_seamless",
  variables = c("temperature_2m","precipitation"))

inflow_merged_precip <- inflow_targets |> 
  filter(variable == "flow_cms_mean" & datetime > lubridate::as_date("2022-07-01") & datetime < lubridate::as_date("2023-06-01")) |> 
  left_join(df_met_precip, by = "datetime") |> 
  filter(!is.na(observation)) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

forecast_met <- df |> 
  filter(variable == "precipitation" ) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(prediction, na.rm = TRUE), .by = c("date", "ensemble")) |> 
  group_by(ensemble) |> 
  mutate(fiveday = RcppRoll::roll_sum(precip, n = 5, fill = NA)) |> 
  rename(datetime = date) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

fit1 = lm(observation ~ month + fiveday, inflow_merged_precip)
summary(fit1)

flow_predicted <- forecast_met |> 
  modelr::add_predictions(fit1)

forecast_flow_df <- flow_predicted |> 
  select(datetime, ensemble, pred) |> 
  rename(prediction = pred,
         parameter = ensemble) |> 
  mutate(parameter = as.numeric(parameter)) |> 
  mutate(variable = "flow_cms_mean") |> 
  na.omit() |> 
  arrange(datetime, parameter) 


####

df_met_temperature <- df_met |> 
  filter(variable == "air_temperature") |> 
  mutate(date = lubridate::as_date(datetime),
         observation = observation - 273.15) |> 
  summarise(temp = mean(observation, na.rm = TRUE), .by = "date") |> 
  mutate(lag = lag(temp),
         tenday = zoo::rollmean(temp, k = 10, align = "right", fill = NA),
         fiveday = zoo::rollmean(temp, k = 5, align = "right", fill = NA),
         twentyday = zoo::rollmean(temp, k = 20, align = "right", fill = NA)) |> 
  rename(datetime = date)

inflow_merged_temp <- inflow_targets |> 
  filter(variable == "temp_c_mean") |> 
  left_join(df_met_temperature, by = "datetime") |> 
  filter(!is.na(observation)) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))

forecast_met <- df |> 
  filter(variable == "temperature_2m" ) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temp = mean(prediction, na.rm = TRUE), .by = c("date", "ensemble")) |> 
  group_by(ensemble) |> 
  mutate(fiveday = RcppRoll::roll_mean(temp, n = 5, fill = NA)) |> 
  rename(datetime = date) |> 
  mutate(month = lubridate::month(datetime),
         season = ifelse(month > 4 & month < 11, "winter", "summer"),
         month = as.factor(month),
         season = as.factor(season))


fit1 = lm(observation ~ fiveday + month, inflow_merged_temp)
summary(fit1)

temp_predicted <-  forecast_met |> 
  modelr::add_predictions(fit1)

forecast_temp_df <- temp_predicted |> 
  select(datetime, ensemble, pred) |> 
  rename(prediction = pred,
         parameter = ensemble) |> 
  mutate(parameter = as.numeric(parameter)) |> 
  mutate(variable = "temp_c_mean") |> 
  na.omit() |> 
  arrange(datetime, parameter)

### combine and submit

forecast_df <- bind_rows(forecast_nutrient_df, forecast_flow_df, forecast_temp_df) |> 
  mutate(project_id = "vera4cast",
         model_id = "inflow_aed",
         family = "enemble",
         site_id = "tubr",
         duration = "P1D") |> 
  filter(datetime > reference_datetime)

ggplot(forecast_df, aes(x = datetime, y = prediction, group= parameter)) + 
  geom_line() + facet_wrap(~variable, scale = "free")

file_name <- paste0("inflow_aed-",Sys.Date(),"csv.gz")

vera4castHelpers::submit(file_name,first_submission = FALSE)


### THIS CODE GOES IN THE LAKE REPO FOR FLARE

#Get forecast from bucket

#Variables that are needed

variables <- c("TP_ugL_sample", "NH4_ugL_sample","NO3NO2_ugL_sample",
               "SRP_ugL_sample","DOC_mgL_sample","DRSI_mgL_sample",
               "TN_ugL_sample", "CH4_umolL_sample", "DIC_mgL_sample",
               "flow_cms_mean", "temp_c_mean")

forecast_df <- NULL

for(i in 1:length(variables)){
  
  s3 <- arrow::s3_bucket(bucket = glue::glue("bio230121-bucket01/vera4cast/forecasts/parquet/duration=P1D/variable={variables[k]}/model_id=inflow_aed/reference_date={reference_date}"),
                         endpoint_override = "https://renc.osn.xsede.org")
  
  df <- arrow::open_dataset(s3) |> filter(site_id == "tubr") |> collect()
  
  forecast_df <- bind_rows(forecast_df, df)
  
}

VARS <- c("parameter", "datetime", "FLOW", "TEMP", "SALT",
          'OXY_oxy',
          'CAR_dic',
          'CAR_ch4',
          'SIL_rsi',
          'NIT_amm',
          'NIT_nit',
          'PHS_frp',
          'OGM_doc',
          'OGM_docr',
          'OGM_poc',
          'OGM_don',
          'OGM_donr',
          'OGM_pon',
          'OGM_dop',
          'OGM_dopr',
          'OGM_pop',
          'PHY_cyano',
          'PHY_green',
          'PHY_diatom')

glm_df_inflow <- forecast_df |> 
  select(datetime, variable, prediction, parameter) |> 
  pivot_wider(names_from = variable, values_from = prediction) |> 
  rename(TEMP = temp_c_mean,
         FLOW = flow_cms_mean) |> 
  dplyr::mutate(NIT_amm = NH4_ugL_sample*1000*0.001*(1/18.04),
                NIT_nit = NO3NO2_ugL_sample*1000*0.001*(1/62.00), #as all NO2 is converted to NO3
                PHS_frp = SRP_ugL_sample*1000*0.001*(1/94.9714),
                OGM_doc = DOC_mgL_sample*1000*(1/12.01)* 0.10,  #assuming 10% of total DOC is in labile DOC pool (Wetzel page 753)
                OGM_docr = 1.5*DOC_mgL_sample*1000*(1/12.01)* 0.90, #assuming 90% of total DOC is in recalcitrant DOC pool
                TN_ugL = TN_ugL_sample*1000*0.001*(1/14),
                TP_ugL = TP_ugL_sample*1000*0.001*(1/30.97),
                OGM_poc = 0.1*(OGM_doc+OGM_docr), #assuming that 10% of DOC is POC (Wetzel page 755
                OGM_don = (5/6)*(TN_ugL_sample-(NIT_amm+NIT_nit))*0.10, #DON is ~5x greater than PON (Wetzel page 220)
                OGM_donr = (5/6)*(TN_ugL_sample-(NIT_amm+NIT_nit))*0.90, #to keep mass balance with DOC, DONr is 90% of total DON
                OGM_pon = (1/6)*(TN_ugL_sample-(NIT_amm+NIT_nit)), #detemined by subtraction
                OGM_dop = 0.3*(TP_ugL_sample-PHS_frp)*0.10, #Wetzel page 241, 70% of total organic P = particulate organic; 30% = dissolved organic P
                OGM_dopr = 0.3*(TP_ugL_sample-PHS_frp)*0.90,#to keep mass balance with DOC & DON, DOPr is 90% of total DOP
                OGM_pop = TP_ugL_sample-(OGM_dop+OGM_dopr+PHS_frp), # #In lieu of having the adsorbed P pool activated in the model, need to have higher complexed P
                CAR_dic = DIC_mgL_sample*1000*(1/52.515),
                OXY_oxy = rMR::Eq.Ox.conc(TEMP, elevation.m = 506, #creating OXY_oxy column using RMR package, assuming that oxygen is at 100% saturation in this very well-mixed stream
                                          bar.press = NULL, bar.units = NULL,
                                          out.DO.meas = "mg/L",
                                          salinity = 0, salinity.units = "pp.thou"),
                OXY_oxy = OXY_oxy *1000*(1/32),
                CAR_ch4 = CH4_umolL_sample, 
                PHY_cyano = 0,
                PHY_green = 0,
                PHY_diatom = 0,
                SIL_rsi = DRSI_mgL_sample*1000*(1/60.08),
                SALT = 0) %>%
  dplyr::mutate_if(is.numeric, round, 4) |> 
  dplyr::select(dplyr::any_of(VARS)) |> 
  tidyr::pivot_longer(-c("datetime","parameter"), names_to = "variable", values_to = "prediction") |>
  dplyr::mutate(model_id = paste0("inflow-aed"),
                site_id = "fcre",
                family = "ensemble",
                flow_type = "inflow",
                flow_number = 1,
                reference_datetime = reference_datetime) |>
  dplyr::select(model_id, site_id, reference_datetime, datetime, family, parameter, variable, prediction, flow_type, flow_number)


glm_df_outflow <- glm_df_inflow |> 
  dplyr::select(datetime, parameter, variable, prediction) |> 
  dplyr::filter(variable %in% c("FLOW","TEMP")) |> 
  dplyr::mutate(model_id = paste0("outflow-aed"),
                site_id = "fcre",
                family = "ensemble",
                flow_type = "outflow",
                flow_number = 1,
                reference_datetime = reference_datetime) |>
  dplyr::select(model_id, site_id, reference_datetime, datetime, family, parameter, variable, prediction, flow_type, flow_number)


glm_df <- bind_rows(glm_df_inflow, glm_df_outflow)

inflow_s3 <- arrow::SubTreeFileSystem$create(file.path(inflow_local_directory, inflow_forecast_path))

arrow::write_dataset(d, path = inflow_s3)








