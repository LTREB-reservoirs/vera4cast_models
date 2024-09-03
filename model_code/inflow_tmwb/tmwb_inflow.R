#calculating flow for BVR using the Thornthwaite-mather water balance model
#modified to daily timestep - added in recharge to help with baseflow underestimation 11Jun2020
#Updated 4Sep2020 - change from GSOD temp/precip data to NLDAS for consistency 
#Updated 23Sep21 - change to obs met data because forecasts look weird when going from glm to FLARE

#packages
if (!require("pacman"))install.packages("pacman")
pacman::p_load(httr, GSODR, curl, elevatr, devtools, sf,
               raster, soilDB, lattice, lubridate, tidyverse)

source('./model_code/inflow_tmwb/EcoHydrology_functions.R')

#download archived EcoHydRology package from github
#install.packages(c("operators", "topmodel", "DEoptim", "XML"))
#install.packages("~/Downloads/EcoHydRology_0.4.12.1.tar.gz", repos = NULL, type = "source")

#soil data
#url="https://websoilsurvey.sc.egov.usda.gov/DSD/Download/AOI/hdbxgq2us0dl32ysxprivmbf/wss_aoi_2023-03-01_08-53-36.zip"
#download.file(url,"mysoil.zip") #Note: will probably have to update wss_aoi date if it's been a while - go to wss homepage and click on start wss link on right of page
#unzip("mysoil.zip")            #zoom in to site, use define aoi tool to select desired area, go to download soils data tab, scroll to bottom of page and click "create download link", right click and copy link address, paste on line 16
list.files()

list.files(paste0(getwd(), "/model_code/inflow_tmwb/FCR_wss_aoi_2024-08-09_10-50-55/spatial"),pattern = "shp")
list.files(paste0(getwd(), "/model_code/inflow_tmwb/FCR_wss_aoi_2024-08-09_10-50-55/tabular"))

objects()
rm(list=objects())

#Using ROANOKE RIVER AT NIAGARA, VA  usgs gage to use as a template (will write over with BVR-specific data) 
flow_start <- '2020-01-01'
#flow_end <- '2024-01-01'
flow_end <- as.character(Sys.Date() + lubridate::days(30))
flow_site <- 'bvre'

myflowgage_id="02056000"
#myflowgage=get_usgs_gage(myflowgage_id,begin_date = flow_start, end_date = flow_end)



### add full get_usgs_gauge function code into script
flowgage_id = myflowgage_id
begin_date = flow_start
end_date = flow_end 

url = paste("http://waterdata.usgs.gov/nwis/inventory?search_site_no=", flowgage_id, "&search_site_no_match_type=exact&sort_key=site_no&group_key=NONE&format=sitefile_output&sitefile_output_format=rdb&column_name=station_nm&column_name=site_tp_cd&column_name=dec_lat_va&column_name=dec_long_va&column_name=alt_va&column_name=drain_area_va&column_name=contrib_drain_area_va&column_name=rt_bol&list_of_search_criteria=search_site_no",sep="")

gage_tsv=readLines(url)
gage_tsv=gage_tsv[grep("^#",gage_tsv,invert=T)][c(1,3)]
tempdf=read.delim(text=gage_tsv,sep="\t",header=T,colClasses = c("character", "character", "numeric", "numeric", "character", "character", "numeric", "numeric", "character", "numeric"))
area = tempdf$drain_area_va* 1.6^2
if(is.na(area)) {area=0;print("warning, no area associated with gage, setting to 0\n")}
declat = tempdf$dec_lat_va
declon = tempdf$dec_long_va
elev = tempdf$alt_va* 12/25.4
if(is.na(elev)) {elev=0;print("warning, no elevation associated with gage, setting to 0\n")}

gagename = tempdf$station_nm
begin_date = as.character(begin_date)
end_date = as.character(end_date)

url = paste("http://nwis.waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&begin_date=",begin_date,"&end_date=",end_date,"&site_no=", flowgage_id, sep = "")
flowdata_tsv=gage_tsv=readLines(url)
flowdata_tsv=flowdata_tsv[grep("^#",flowdata_tsv,invert=T)][c(3:length(flowdata_tsv))]
flowdata = read.delim(text=flowdata_tsv,header=F,sep="\t",col.names = c("agency", "site_no", "date", "flow", "quality"), colClasses = c("character", "numeric", "character", "character", "character"), fill = T)
flowdata$mdate = as.Date(flowdata$date, format = "%Y-%m-%d")
flowdata$flow = as.numeric(as.character(flowdata$flow)) * 12^3 * 2.54^3/100^3 * 24 * 3600
flowdata = na.omit(flowdata)
returnlist = list(declat = declat, declon = declon, flowdata = flowdata, area = area, elev = elev, gagename = gagename)

myflowgage = returnlist

### end function code ###




#change coordinates and area for entire BVR watershed
myflowgage$area<- 2.27 #km
myflowgage$declat<- 37.31321
myflowgage$declon<- -79.81535

# all_results <- arrow::open_dataset("s3://anonymous@bio230121-bucket01/flare/drivers/met/gefs-v12/stage2?endpoint_override=renc.osn.xsede.org")
# df <- all_results |> dplyr::collect()

## historic met
NLDAS <- arrow::open_dataset(arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=",flow_site),
                                   endpoint_override = 'renc.osn.xsede.org',
                                   anonymous = TRUE)) |>
  filter(variable %in% c('precipitation_flux', "air_temperature")) |> 
  collect() |> 
  group_by(datetime, variable) |> 
  summarise(mean_prediction = mean(prediction)) |> 
  ungroup() |> 
  pivot_wider(names_from = 'variable', values_from = 'mean_prediction') |> 
  rename(AirTemp = air_temperature, Rain = precipitation_flux)


#use NLDAS for missing met days 
#NLDAS<- read.csv("./inputs/BVR_GLM_NLDAS_010113_123121_GMTadjusted.csv")
#NLDAS[is.na(NLDAS)]=0 # A Quick BUT sloppy removal of NAs

#convert NLDAS date to as.date format
NLDAS$time <-as.Date(NLDAS$datetime)
#convert rain from m/d to mm/d 
NLDAS$precip_mm <-NLDAS$Rain * 1000 #/ 24

#average by date
NLDAS <- NLDAS %>% 
  dplyr::select(time, AirTemp, precip_mm) %>% 
  group_by(time) %>%
  rename(mdate=time) %>%
  summarise(MaxTemp_C = max(AirTemp),
            MinTemp_C = min(AirTemp),
            MeanTemp_C = mean(AirTemp),
            Precip_mmpd = sum(precip_mm)) 

#replace flow with NAs because this is specific to Roanoke River (not BVR)
myflowgage$flowdata[["flow"]] <- NA

# Merge met_final weather data with flow gage to use as our base HRU data structure
myflowgage$TMWB=merge(myflowgage$flowdata,NLDAS)

# Grab the necessary soil and elevation spatial layers and parameters (usgs)
#url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/HU8/HighResolution/Shape/NHD_H_03010101_HU8_Shape.zip"
#curl_download(url,"NHD_H_03010101_HU8_Shape.zip")
#unzip("NHD_H_03010101_HU8_Shape.zip",exdir="03010101") 

#set coordinates to plot DEM raster
degdist=sqrt(myflowgage$area*4)/80
mybbox = matrix(c(
  myflowgage$declon - degdist, myflowgage$declon + degdist, 
  myflowgage$declat - degdist, myflowgage$declat + degdist), 
  ncol = 2, byrow = TRUE)

streams=readOGR(paste0(getwd(), "/model_code/inflow_tmwb/TMWB_data/03010101/Shape/NHDFlowline.dbf")) 
streams=readOGR(paste0(getwd(), "/model_code/inflow_tmwb/TMWB_data/03010101/Shape/NHDFlowline.dbf")) 

mysoil <- readOGR(file.path(getwd(), "/model_code/inflow_tmwb/TMWB_data/soils"))

# Associate mukey with cokey from component
mukey_statement = format_SQL_in_statement(unique(mysoil$mukey))
q_mu2co = paste("SELECT mukey,cokey FROM component WHERE mukey IN ", mukey_statement, sep="")
mu2co = SDA_query(q_mu2co)

# Second associate cokey with ksat_r,awc_r,hzdepb_r from chorizon
cokey_statement = format_SQL_in_statement(unique(mu2co$cokey))
q_co2ch = paste("SELECT cokey,ksat_r,awc_r,hzdepb_r  FROM chorizon WHERE cokey IN ", cokey_statement, sep="")
co2ch = SDA_query(q_co2ch)

# Aggregate max values of ksat_r,awc_r, and hzdepb_r
mu2ch=merge(mu2co,co2ch)
mu2chmax=aggregate(mu2ch,list(mu2ch$mukey),max)

#set projection
proj4string(streams)
proj4string(mysoil)<- "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"

#convert to sf
mysoil <- st_as_sf(mysoil)

# Use the spatial extents from our stream to download elevation raster.
mydem=get_elev_raster(mysoil, z = 11, src ="aws",clip="bbox")

#view watershed
# plot(mydem)
# lines(mysoil,col="black")
# points(myflowgage$declon,myflowgage$declat,pch = 24, cex=2, col="blue", bg="red", lwd=2)

# For initializing slopes, we store the summary stats for terrain slope
slope_sum=summary(terrain(mydem, opt='slope',unit = "radians"))

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

#need to modify a couple of the functions bc EcoHydRology is no longer maintained
source("./model_code/inflow_tmwb/EcoHydrology_functions.R")

TMWBModel<-function(hru_list){  
  # hru_list is the same object we have been using till now to store all our
  # variables and parameters.
  myflowgage=hru_list
  attach(myflowgage)
  attach(TMWB)
  
  # Snow accumulation and melt, as well as PET only depend on the surface attributes, and as such, can run  at the beginning, independent of the daily calculated ET, TMWB, and the linear reservoir Storage Discharge (Qmm). 
  SNO_Energy=snowmelt(mdate, Precip_mmpd, MaxTemp_C-3, MinTemp_C-3, myflowgage$declat, 
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
  PET=pet_fromTemp(Jday=(1+as.POSIXlt(mdate)$yday),Tmax_C=MaxTemp_C,Tmin_C = MinTemp_C, lat_radians = myflowgage$declat*pi/180) * 1000
  myflowgage$TMWB$PET=PET
  
  # Those processes that are dependant on prior days conditions, we run as a loop through each of the days.
  for (t in 2:length(AW)){
    ET[t] = min (AW[t-1],PET[t]*AW[t-1]/AWC[t-1]) 
    # Calculating Net Precipitation 
    dP[t] = Precip_mmpd[t] - SnowfallWatEq_mm[t] - ET[t] + SnowMelt_mm[t]
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
    if(Precip_mmpd[t]>0) {Drainage[t]<- Precip_mmpd[t] - ExcessOut[t] - ET[t]} #recharge equation from Shuler and Mariner 2020
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

# Call the new TMWBModel() function 
TMWBsol=TMWBModel(myflowgage)
# Convert area from km to m (10^6) and Qpred from mm to m (10^-3) 
TMWBsol$TMWB$Qpred_m3pd=TMWBsol$TMWB$Qpred*TMWBsol$area*10^3
# Convert Qpred_m3pd to Qpred_m3ps (1m3/s = 86400 m3/d)
TMWBsol$TMWB$Qpred_m3ps=TMWBsol$TMWB$Qpred_m3pd/86400

#plots to visualize data
# plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$Qpred_m3pd,col="red", type='l')
# plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$Qpred_m3ps,col="orange", type='l')
# plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$ExcessOut,col="blue", type='l')
# plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$S,col="green", type='l')
# plot(TMWBsol$TMWB$mdate,TMWBsol$TMWB$Drainage,col="purple", type='l')

if (!file.exists("./model_output/inflow_tmwb/")){
  dir.create("./model_output/inflow_tmwb/")
}

#create csv for q export
QExport<- data.frame("time"=TMWBsol$TMWB$mdate, "Q_m3pd"=TMWBsol$TMWB$Qpred_m3pd)
write.csv(QExport, "model_output/inflow_tmwb/Flow_calcs_met.csv", row.names = F)


## MAKE INFLOW FORECAST
message('Generating Inflow Forecast...')
source('./model_code/inflow_tmwb/create_inflow_forecast.R')

site = 'bvre'
model_name = 'tmwb_inflow'
forecast_date <- Sys.Date()


inflow_forecast <- create_inflow_forecast(inflow_obs = 'model_output/inflow_tmwb/Flow_calcs_met.csv',
                           site_id = site,
                           model_name = model_name,
                           forecast_date = forecast_date,
                           noaa_date = Sys.Date() - lubridate::days(1), # date for NOAA forecasts
                           soil_file_dir = './model_code/inflow_tmwb/TMWB_data/', # directory where you store your previously downloaded soil files
                           forecast_start_day = Sys.Date() - lubridate::days(1), # day forecasts start
                           output_dir = 'model_output/inflow_tmwb/', # where you want to store the inflow forecast files which are created by this function
                           inflow_process_uncertainty = TRUE, # true of false (I removed this for my forecasting purposes)
                           config = NULL,
                           s3_mode = FALSE,
                           bucket = NULL)

# Write the file locally
forecast_file_abs_path <- paste0("./model_output/inflow_tmwb/", model_name, "_", site, "_", forecast_date, ".csv")

# write to file
print('Writing File...')

if (!file.exists("./model_output/inflow_tmwb/")){
  dir.create("./model_output/inflow_tmwb/")
}

write.csv(inflow_forecast, forecast_file_abs_path, row.names = FALSE)


## validate and submit forecast

# validate
print('Validating File...')
vera4castHelpers::forecast_output_validator(forecast_file_abs_path)
vera4castHelpers::submit(forecast_file_abs_path, s3_region = "submit", s3_endpoint = "ltreb-reservoirs.org", first_submission = FALSE)

