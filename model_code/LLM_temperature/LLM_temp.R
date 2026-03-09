Sys.setenv("GEMINI_API_KEY" = Sys.getenv("CCC_GEMINI_API_KEY"))

library(pacman)
pacman::p_load(readr, tidyverse, usethis, ellmer, glue, vera4castHelpers)

#need to train LLM
targets <- readr::read_csv('https://amnh1.osn.mghpcc.org/bio230121-bucket01/vera4cast/targets/project_id=vera4cast/duration=P1D/daily-insitu-targets.csv.gz') %>%
  filter(site_id == 'fcre', variable=="Temp_C_mean", depth_m==1.6)# datetime==Sys.Date()-1)

current_obs <- targets |>
  filter(datetime ==Sys.Date()-1) |>
  select(observation)

yesterday_obs <- targets |>
  filter(datetime ==Sys.Date()-2) |>
  select(observation)

previous_obs <- targets |>
  filter(datetime ==Sys.Date()-3) |>
  select(observation)

my_prompt <- glue::glue("data with 35 rows and 2 columns, x and y. x should be water temperature for a lake in Vinton, Virginia this month and y should have standard deviation of the prediction. Each row is a sequential day of data. Use the air temperature forecast for Vinton in degrees Celsius to make the water temperature forecast in degrees Celsius. The water temperature today is {current_obs}. Yesterday's temperature is {yesterday_obs}. The day before's temperature is {previous_obs}.")

ensemble<- 8 #testing the number of ensemble members
forecast<-as.data.frame(matrix(data=NA,nrow=35,ncol=ensemble))
#creating a dataframe to collect the forecasts

for(i in 1:ensemble){
  print(i)
  
  chat <- chat_google_gemini(
    model = "gemini-2.5-flash",
    system_prompt = "Given a description, generate structured data."
  )
  
  response <-
    chat$chat(
      my_prompt,
      echo = FALSE
    )
  
  df <-
    chat$chat_structured(
      response,
      type = type_array(
        items = type_object(
          x = type_number(),
          y = type_string()
        )
      )
    )
  
  forecast[,i] <- df[,1]
}

forecast <- forecast |> 
  mutate(datetime = Sys.Date() + row_number() - 1)

#format forecast for submission
output_file_name <- paste0("inflow_gefsClimAED-",reference_datetime,".csv.gz")

forecast_formatted <- forecast |> 
  rowwise() %>%  
  mutate(    
    mu = median(c_across(where(is.numeric)), na.rm = TRUE), #note that I'm doing medians (not means!) to control for ensemble members that went awry
    sigma = sd(c_across(where(is.numeric)), na.rm = TRUE)) %>%  
  ungroup() |> #calculate the mean prediction + SD across forecasts 
  mutate(project_id = "vera4cast",
         model_id = "llm.gemini2.5flash.ccc",
         reference_datetime = lubridate::as_datetime(Sys.Date()),
         # reference_datetime = now(tzone = "UTC") %>%
         #   with_tz("UTC") %>%
         #   force_tz("UTC"),
         #reference_datetime = lubridate::as_datetime(reference_datetime),
         duration = "P1D",
         site_id = "fcre",
         depth_m = 1.6,
         family = "normal",
         variable = "Temp_C_mean",
         datetime = as.POSIXct(datetime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")) |>  
  pivot_longer(    
    cols = c(mu, sigma), #the columns to combine    
    names_to = "parameter", #becomes "mu" or "sigma"    
    values_to = "prediction" #the numeric values  
  ) |> 
  select(model_id, datetime, reference_datetime, site_id, variable, family, parameter, prediction, depth_m, project_id, duration) |> 
  readr::write_csv(output_file_name)

vera4castHelpers::forecast_output_validator(output_file_name)
vera4castHelpers::submit(file_name,first_submission = TRUE)