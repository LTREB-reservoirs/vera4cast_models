#Format data for each model and fit models from 2018-2021
#Author: Mary Lofton
#Date: 26SEP23

#Purpose: format historical FCR + BVR data for input into forecast models

library(tidyverse)
library(lubridate)

#Load data formatting functions
data.format.functions <- list.files("./model_code/R/fable_chla/function_library/format_data")
sapply(paste0("./model_code/R/fable_chla/function_library/format_data", data.format.functions),source,.GlobalEnv)

#Define targets filepath
targets <- "https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz"

#Define start and end dates (needed for interpolation)
end_date = curr_reference_datetime

#Format data
dat_ETS <- format_data_ETS(targets = targets,
                           end_date = end_date)
dat_ARIMA <- format_data_ARIMA(targets = targets,
                           end_date = end_date)
dat_NNETAR <- format_data_NNETAR(targets = targets,
                           end_date = end_date)
dat_binary <- format_data_binary(targets = targets,
                                 end_date = end_date)

#Write processed data to file
write.csv(dat_ETS, "./model_output/fable_chla/processed_targets/ETS.csv",row.names = FALSE)
write.csv(dat_ARIMA, "./model_output/fable_chla/processed_targets/ARIMA.csv",row.names = FALSE)
write.csv(dat_NNETAR, "./model_output/fable_chla/processed_targets/NNETAR.csv",row.names = FALSE)

