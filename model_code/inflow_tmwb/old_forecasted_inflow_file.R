# function to use noaa forecasts to drive TWMB watershed model and produce ensemble forecasts for BVR inflows

forecast_inflow_bvre <- function(inflow_obs, #observed inflow file (in this case the TWMB output using obs met)
                                 site_id, # 4 digit side code, can come from config
                                 folder, # working directory
                                 forecast_dir, # directory where NOAA files are stored, root should be the forecast date, e.g. "C:/Users/wwoel/Desktop/FLARE_AR_CHLA/ARIMA_working/drivers/noaa/NOAAGEFS_1hr/fcre/2020-08-01"
                                 soil_file_dir, # directory where you store your previously downloaded soil files
                                 forecast_start_day, # day forecasts start
                                 output_dir, # where you want to store the inflow forecast files which are created by this function
                                 inflow_model, # name of your inflow model, goes into file name
                                 inflow_process_uncertainty = TRUE, # true of false (I removed this for my forecasting purposes)
                                 config = NULL,
                                 s3_mode = FALSE,
                                 bucket = NULL) {
  
  if (!require("pacman"))install.packages("pacman")
  pacman::p_load(httr,EcoHydRology,GSODR,curl,elevatr,raster,soilDB,rgdal,lattice,lubridate, tidyverse)
  library(magrittr)
  
  
  inflow <- readr::read_csv(inflow_obs, col_types = readr::cols()) 
  
  lake_name_code <- site_id
  
  lake_directory <- folder
  
  curr_all_days <- NULL
  
  # get list of noaa forecast files
  forecast_dir_hour <- paste0(forecast_dir, '/00')
  forecast_files <- list.files(forecast_dir_hour, pattern = ".nc", full.names = TRUE)
  if(length(forecast_files) < 1){
    forecast_dir_hour <- paste0(forecast_dir, '/06')
    forecast_files <- list.files(forecast_dir_hour, pattern = ".nc", full.names = TRUE)
    
  }
  if(length(forecast_files) < 1){
    forecast_dir_hour <- paste0(forecast_dir, '/12')
    forecast_files <- list.files(forecast_dir_hour, pattern = ".nc", full.names = TRUE)
    
  }
  
  
  
  #------------------------------------------------------------------------------#
  #      Thornthwaite-Mather Water Balance Model for Forecasting Inflow          #
  #------------------------------------------------------------------------------#      
  
  
  #Using ROANOKE RIVER AT NIAGARA, VA  usgs gage to use as a template (will write over with BVR-specific data) 
  myflowgage_id <- "02056000"
  end_date <- as.Date(forecast_start_day) + 40
  myflowgage <- get_usgs_gage(myflowgage_id,
                              begin_date = as.Date(forecast_start_day),
                              end_date = end_date) # 16 days from forecast_start_day 
  
  #change coordinates and area for entire BVR watershed
  myflowgage$area <- 2.27 #km
  myflowgage$declat <- 37.31321
  myflowgage$declon <- -79.81535
  
  #replace flow with NAs because this is specific to Roanoke River (not BVR)
  myflowgage$flowdata[["flow"]] <- NA
  
  #set coordinates to plot DEM raster
  degdist=sqrt(myflowgage$area*4)/200
  mybbox = matrix(c(
    myflowgage$declon - degdist, myflowgage$declon + degdist, 
    myflowgage$declat - degdist, myflowgage$declat + degdist), 
    ncol = 2, byrow = TRUE)
  
  streams <- rgdal::readOGR(paste0(soil_file_dir, "/NHD_H_03010101_HU8_Shape/Shape/NHDFlowline.dbf")) 
  
  #mysoil <- soilDB::mapunit_geom_by_ll_bbox(mybbox)
  #writeOGR(obj=mysoil, dsn="soils", layer="mysoil", driver="ESRI Shapefile")
  mysoil <- rgdal::readOGR(paste0(soil_file_dir,  "/mysoil/wss_aoi_2021-10-13_12-59-06/spatial/soilmu_a_aoi.dbf"))
  
  # Associate mukey with cokey from component
  mukey_statement <- soilDB::format_SQL_in_statement(unique(mysoil$MUKEY))
  q_mu2co = paste("SELECT mukey,cokey FROM component WHERE mukey IN ", mukey_statement, sep="")
  mu2co = soilDB::SDA_query(q_mu2co)
  
  # Second associate cokey with ksat_r,awc_r,hzdepb_r from chorizon
  cokey_statement = soilDB::format_SQL_in_statement(unique(mu2co$cokey))
  q_co2ch = paste("SELECT cokey,ksat_r,awc_r,hzdepb_r  FROM chorizon WHERE cokey IN ", cokey_statement, sep="")
  co2ch = soilDB::SDA_query(q_co2ch)
  
  # Aggregate max values of ksat_r,awc_r, and hzdepb_r
  mu2ch=merge(mu2co,co2ch)
  mu2chmax=aggregate(mu2ch,list(mu2ch$mukey),max)
  
  #set projection
  sp::proj4string(streams)
  sp::proj4string(mysoil) <- "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"
  
  # Use the spatial extents from our stream to download elevation raster.
  mydem <- elevatr::get_elev_raster(mysoil, z = 11, src ="aws",clip="bbox")
  
  # For initializing slopes, we store the summary stats for terrain slope
  slope_sum <- summary(terrain(mydem, opt='slope',unit = "radians"))
  
  # 3 Functions to calculate SWE and excess when soil is drying, wetting, and wetting above capacity
  soildrying<-function(AWprev,dP,AWC){
    AW<-AWprev*exp(dP/AWC)
    excess<-0.0
    c(AW,excess)
  }
  
  soil_wetting_above_capacity<-function(AWprev,dP,AWC){
    AW<-AWC
    excess<-AWprev+dP-AWC
    c(AW,excess)
  }
  
  soilwetting<-function(AWprev,dP,AWC){
    AW<-AWprev+dP
    excess<-0.0
    c(AW,excess)
  }
  
  if(forecast_start_day > as.Date('2020-09-24')){
    z <- 2
  }else(z <- 1)
  
  for(j in z:length(forecast_files)){
    
    ens <- dplyr::last(unlist(stringr::str_split(basename(forecast_files[j]),"_")))
    ens <- stringr::str_sub(ens,1,5)
    noaa_met_nc <- ncdf4::nc_open(forecast_files[j])
    noaa_met_time <- ncdf4::ncvar_get(noaa_met_nc, "time")
    origin <- stringr::str_sub(ncdf4::ncatt_get(noaa_met_nc, "time")$units, 13, 28)
    origin <- lubridate::ymd_hm(origin)
    noaa_met_time <- origin + lubridate::hours(noaa_met_time)
    AirTemp <- ncdf4::ncvar_get(noaa_met_nc, "air_temperature")
    Rain <- ncdf4::ncvar_get(noaa_met_nc, "precipitation_flux")
    ncdf4::nc_close(noaa_met_nc)
    noaa_met <- tibble::tibble(time = noaa_met_time,
                               AirTemp = AirTemp,
                               Rain = Rain)
    
    #noaa_met <- rbind(obs_met, noaa_met)
    
    curr_met_daily <- noaa_met %>%
      dplyr::mutate(AirTemp = AirTemp - 273.15,
                    Rain = Rain * (60 * 60 * 24)) %>% #was /1000 to get to m/d, but SnowMelt needs mm/d units
      dplyr::mutate(mdate = lubridate::as_date(time)) %>%
      dplyr::group_by(mdate) %>%
      dplyr::summarize(Rain = mean(Rain),
                       AirTemp = mean(AirTemp),
                       MaxTemp_C = max(AirTemp),
                       MinTemp_C = min(AirTemp),.groups = 'drop') %>%
      dplyr::mutate(ensemble = ens) %>%
      dplyr::mutate(AirTemp_lag1 = dplyr::lag(AirTemp, 1),
                    Rain_lag1 = dplyr::lag(Rain, 1),
                    MaxTemp_lag1 = dplyr::lag(MaxTemp_C, 1),
                    MinTemp_lag1 = dplyr::lag(MinTemp_C, 1)) %>%
      dplyr::slice(-1) %>%
      dplyr::mutate(FLOW = NA,
                    TEMP = NA)
    
    
    #curr_met_daily$TEMP[1] <- init_flow_temp$TEMP
    
    # if(inflow_process_uncertainty == TRUE){
    #   inflow_error <- rnorm(nrow(curr_met_daily), 0, config$future_inflow_flow_error)
    #   temp_error <- rnorm(nrow(curr_met_daily), 0, config$future_inflow_temp_error)
    # }else{
    #   inflow_error <- rep(0.0, nrow(curr_met_daily))
    #   temp_error <- rep(0.0, nrow(curr_met_daily))
    # }
    
    # Merge the NOAA weather data with flow gage to use as our base HRU data structure
    myflowgage$TMWB <- merge(myflowgage$flowdata,curr_met_daily)
    
    #initialize parameters
    myflowgage$TMWB$AWC=0.13*400 #AWC=.13; 0.12 and 0.16 were the values obtained from USDA web soil survey
    # z=2000mm --> this one is hard because it really changes Qpred a LOT - calibrate this parameter? trees generally have <3500 mm roots...
    myflowgage$TMWB$dP = 0 # Net precip
    myflowgage$TMWB$ET = 0 # Evapotranspiration
    myflowgage$TMWB$Albedo=.23
    myflowgage$TMWB$PET = 0 # Potential evapotranspiration
    myflowgage$TMWB$AW =  100 # Available water
    myflowgage$TMWB$SnowMelt_mm = 0 
    myflowgage$TMWB$SnowfallWatEq_mm = 0 # New snow
    myflowgage$TMWB$SnowWaterEq_mm = 0  # Snow depth
    myflowgage$TMWB$ExcessOut = 0 # Excess going out (runoff)
    myflowgage$TMWB$Drainage = 0
    myflowgage$TMWB$Qpred=NA
    myflowgage$TMWB$Qpred[1]=0
    myflowgage$TMWB$S=NA
    myflowgage$TMWB$S[1]=0
    myflowgage$fcres=0.3  #typically ranges from 0.2-0.5
    myflowgage$SlopeRad=0.0 
    
    TMWBModel<-function(hru_list){  
      # hru_list is the same object we have been using till now to store all our
      # variables and parameters.
      myflowgage=hru_list
      attach(myflowgage)
      attach(TMWB)
      
      # Snow accumulation and melt, as well as PET only depend on the surface attributes, and as such, can run  at the beginning, independent of the daily calculated ET, TMWB, and the linear reservoir Storage Discharge (Qmm). 
      SNO_Energy <- EcoHydRology::SnowMelt(mdate, Rain_lag1, MaxTemp_lag1-3, MinTemp_lag1-3, myflowgage$declat, 
                                           slope = 0, aspect = 0, tempHt = 1, windHt = 2, groundAlbedo = 0.25,
                                           SurfEmissiv = 0.95, windSp = 2, forest = 0, startingSnowDepth_m = 0,
                                           startingSnowDensity_kg_m3=450)
      
      SnowMelt_mm=SNO_Energy$SnowMelt_mm     
      SnowWaterEq_mm=SNO_Energy$SnowWaterEq_mm 
      SnowfallWatEq_mm=SNO_Energy$SnowfallWatEq_mm
      myflowgage$TMWB$SnowMelt_mm=SnowMelt_mm
      myflowgage$TMWB$SnowWaterEq_mm=SnowWaterEq_mm
      myflowgage$TMWB$SnowfallWatEq_mm=SnowfallWatEq_mm
      myflowgage$TMWB$Albedo[myflowgage$TMWB$SnowfallWatEq_mm>0]=.95
      PET=PET_fromTemp(Jday=(1+as.POSIXlt(mdate)$yday),Tmax_C=MaxTemp_C,Tmin_C = MinTemp_C, lat_radians = myflowgage$declat*pi/180) * 1000
      myflowgage$TMWB$PET=PET
      
      # Those processes that are dependant on prior days conditions, we run as a loop through each of the days.
      for (t in 2:length(AW)){
        ET[t] = min (AW[t-1],PET[t]*AW[t-1]/AWC[t-1]) 
        # Calculating Net Precipitation 
        dP[t] = Rain_lag1[t] - SnowfallWatEq_mm[t] - ET[t] + SnowMelt_mm[t]
        # TMWB Solution
        if (dP[t]<=0) {
          values<-soildrying(AW[t-1],dP[t],AWC[t])
        } else if((dP[t]>0) & (AW[t-1]+dP[t])<=AWC[t]) {
          values<-soilwetting(AW[t-1],dP[t],AWC[t])
        } else{
          values <- soil_wetting_above_capacity(AW[t-1],dP[t],AWC[t])
        }
        AW[t]<-values[1] 
        ExcessOut[t]<-values[2] #this is essentially just runoff 
        if(Rain_lag1[t]>0) {Drainage[t]<- Rain_lag1[t] - ExcessOut[t] - ET[t]} #recharge equation from Shuler and Mariner 2020
        if(Drainage[t]<0){ Drainage[t]<- 0}
        S[t]=S[t-1]+ExcessOut[t] + Drainage[t]
        Qpred[t]=fcres*S[t]  #Q as calculated from TMWB model (seems to underestimate baseflow without adding in recharge component)
        S[t]<-S[t]-Qpred[t] 
        print(t)
      }
      
      # UPDATE all the calculated vectors for list to be returned from function
      # BEFORE DETACHING
      myflowgage$TMWB$SnowMelt_mm=SnowMelt_mm
      myflowgage$TMWB$SnowWaterEq_mm=SnowWaterEq_mm
      myflowgage$TMWB$SnowfallWatEq_mm=SnowfallWatEq_mm
      myflowgage$TMWB$Albedo[myflowgage$TMWB$SNO>0]=.95
      myflowgage$TMWB$dP=dP
      myflowgage$TMWB$AW=AW
      myflowgage$TMWB$ExcessOut=ExcessOut
      myflowgage$TMWB$Drainage=Drainage
      myflowgage$TMWB$S=S
      myflowgage$TMWB$PET=PET
      myflowgage$TMWB$ET=ET
      myflowgage$TMWB$Qpred=Qpred 
      detach(TMWB)
      detach(myflowgage)
      # Return the updated list.
      return(myflowgage)
    }
    
    # run the TMWBModel
    TMWBsol <- TMWBModel(myflowgage)
    # Convert area from km to m (10^6) and Qpred from mm to m (10^-3) 
    TMWBsol$TMWB$Qpred_m3pd=TMWBsol$TMWB$Qpred*TMWBsol$area*10^3 #* 0.05 #trying to manually scale down just to get glm to run - will eventually calibrate pars to get better inflow estimates
    # Convert Qpred_m3pd to Qpred_m3ps (1m3/s = 86400 m3/d)
    TMWBsol$TMWB$Qpred_m3ps=TMWBsol$TMWB$Qpred_m3pd/86400
    
    plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$Qpred_m3ps,col="red", type='l')
    
    #forecasted inflow
    curr_met_daily$FLOW = TMWBsol$TMWB$Qpred_m3ps
    
    curr_met_daily$TEMP[1] <- curr_met_daily$AirTemp[1]  #pulled coeffs from old config file - should actually calculate these for bvr (not fcr)
    for(i in 2:nrow(curr_met_daily)){                                                  
      curr_met_daily$TEMP[i] = 0.20291 +
        0.94214 * curr_met_daily$AirTemp[i-1] +
        0.04278 * curr_met_daily$AirTemp_lag1[i] + 0.943
    }
    
    #change mdate col back to time
    curr_met_daily <- rename(curr_met_daily, time = mdate)
    
    curr_met_daily <- curr_met_daily %>%
      dplyr::mutate(FLOW = ifelse(FLOW < 0.0, 0.0, FLOW))
    
    curr_met_daily <- curr_met_daily %>%
      dplyr::mutate(SALT = 0.0) %>%
      dplyr::select(time, FLOW, TEMP, SALT, AirTemp, Rain) %>% #, OXY_oxy
      dplyr::mutate_at(dplyr::vars(c("FLOW", "TEMP", "SALT")), list(~round(., 4))) %>% #,"OXY_oxy"
      dplyr::mutate(type = "inflow",
                    inflow_num = 1) %>%
      slice(-1)
    
    curr_met_daily_output <- curr_met_daily %>% #this assumes that outflow = inflow, but might want to think about this a bit
      dplyr::select(time, FLOW, TEMP) %>% #, OXY_oxy
      dplyr::mutate(type = "outflow",
                    outflow_num = 1)
    
    forecast_date <- forecast_start_day
    end_date <- dplyr::last(curr_met_daily$time)
    
    # add a dummy row for the forecast_start_day for correct indexing within fcr forecast 
    curr_met_daily[nrow(curr_met_daily) + 1, 1] <- as.Date(forecast_start_day)
    curr_met_daily <- curr_met_daily[order(curr_met_daily$time),]
    
    
    identifier_inflow <- paste0("INFLOW-", inflow_model,"_", lake_name_code, "_", format(forecast_start_day, "%Y-%m-%d"),"_",
                                format(end_date, "%Y-%m-%d"))
    
    identifier_outflow <- paste0("OUTFLOW-", inflow_model,"_", lake_name_code, "_", format(forecast_start_day, "%Y-%m-%d"),"_",
                                 format(end_date, "%Y-%m-%d"))
    
    inflow_file_name <- file.path(output_dir, paste0(identifier_inflow,"_", ens, ".csv"))
    outflow_file_name <- file.path(output_dir, paste0(identifier_outflow,"_", ens, ".csv"))
    
    readr::write_csv(x = curr_met_daily,
                     file = inflow_file_name)
    
    #readr::write_csv(x = curr_met_daily_output,
    #                 file = outflow_file_name)
    
    
  }
  return(list(identifier_inflow))
}
