
# 1. Load the required packages
library(tidyverse)
library(lubridate)
library(vera4castHelpers)

# 2. Set arguments
my_forecast_date <- Sys.Date()
my_model_id <- 'Dom_CO2_Model'

# 3. Source the math function from your R folder
source('/vera4cast_models/R/Dom_CO2/Dom_CO2_model.R')

# 4. Run the function!
final_forecast <- Dom_CO2model_function(forecast_date = my_forecast_date, model_id = my_model_id)

# View the results
print(head(final_forecast))

#  Save it to submit
# file_name <- paste0("vera4cast-", my_model_id, "-", my_forecast_date, ".csv")
# write_csv(final_forecast, file_name)