
# 1. Load the required packages
library(tidyverse)
library(lubridate)
library(vera4castHelpers)

# 2. Set arguments
my_forecast_date <- Sys.Date() - lubridate::days(1)
my_model_id <- 'co2_insitu_lm_Dom'

# 3. Source the math function from your R folder
source('./R/co2_insitu_lm_Dom/CO2_model_function.R')

# 4. Run the function!
final_forecast <- Dom_CO2model_function(forecast_date = my_forecast_date, model_id = my_model_id)

# save and submit
write.csv(final_forecast, './model_output/Dom_Co2/CO2_df.csv')

vera4castHelpers::forecast_output_validator('./model_output/Dom_Co2/CO2_df.csv')

vera4castHelpers::submit('./model_output/Dom_Co2/CO2_df.csv', s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)