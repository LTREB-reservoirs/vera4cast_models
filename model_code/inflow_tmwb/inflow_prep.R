#originally written by CCC on 16 July 2018 to create weir and wetland inflow files + outflow for FCR GLM model
#updated 1 June 2020 to be made "tidy" and update nutrient fractions for inflows
# Updated for BVR GLM inflow files: A Hounshell, 18 Jun 2020
# Updated 08 July 2020 per FCR inflow file: Added DONR and DOPR fractions for inflow prep
# Updated 17 Sep 2020 to include updated inflow model using NLDAS data
# Updated 23 Feb 2021 to include new inflow file (BVR_flow_calcs_new)
# Updated 16 Mar 2021 to include a test land-use scenario
# Extended met obs file and remade inflows 5May2023 HLW

# wd <- getwd()
# setwd(wd)
# sim_folder <- getwd()

# First, read in inflow file generated from Thronthwaite Overland flow model + groundwater recharge
# From HW: for entire watershed; units in m3/d
# Updated inflow model using FCR met station precip and temp data: units in m3/d - need to convert to m3/s
# inflow <- read_csv("./model_output/inflow_tmwb/Flow_calcs_met.csv")
# inflow$time = as.POSIXct(strptime(inflow$time,"%Y-%m-%d", tz="EST"))
# #inflow <- inflow[,-c(1)]
# names(inflow)[2] <- "FLOW"
# inflow$FLOW = inflow$FLOW/86400

# inflow_start <- min(inflow$time)
# inflow_end <- max(inflow$time) + lubridate::days(30)

#diagnostic plot
#plot(inflow$time, inflow$FLOW)

# Need to append water temperature to inflow file (as TEMP)
# Based on comparisons btw BVR inflow (100,200) temp from RC days and FCR inflow (100) (see BVR_Inflow_Temp.R),
# going to assume temperature measured at FCR 100 is close to BVR inflow temp

# # Download FCR inflow data from EDI
# inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/202/13/da4beae4df936f513ac7600d44871c07" 
# infile1 <- tempfile()
# try(download.file(inUrl1,infile1))
# if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")
# 
# temp <- read.csv(infile1)
# temp$DateTime = as.POSIXct(strptime(temp$DateTime,"%Y-%m-%d", tz="EST"))
# temp <- temp |>  
#   dplyr::select(DateTime, WVWA_Temp_C) |>  
#   rename(time=DateTime, TEMP=WVWA_Temp_C) |> 
#   dplyr::filter(time >= as.POSIXct(inflow_start) & time <= as.POSIXct(inflow_end)) |>  
#   group_by(time) |>  
#   summarise(TEMP=mean(TEMP)) #gives averaged daily temp in C

# # Merge inflow and inflow temp datasets
# inflow <- merge(inflow,temp,by="time",all=TRUE) 
# 
# #only select data until last inflow obs so can infill the missing days
# inflow <- inflow[inflow$time<=last(temp$time),]
# 
# #fill in missing days
# inflow <- inflow |> mutate(TEMP=na.fill(na.approx(TEMP),"extend")) 
# 
# # Add SALT column (salinty = 0 for all time points)
# inflow <- inflow |> mutate(SALT = rep(0,length(inflow$time)))

# #some diagnostic plots of inflow
# plot(inflow$time, inflow$FLOW, type = "o")
# plot(inflow$time, inflow$TEMP, type = "l", col = "red")


#######################################################################

#load packages
pacman::p_load(dplyr,zoo,EcoHydRology,tidyverse,lubridate) #rMR

source('./model_code/inflow_tmwb/rMR_functions.R')

inflow_prep <- function(inflow_df){

## start new script
forecasted_inflow <- inflow_df
forecasted_inflow$time = as.POSIXct(strptime(forecasted_inflow$time,"%Y-%m-%d", tz="EST"))
#inflow <- inflow[,-c(1)]
forecasted_inflow$FLOW = forecasted_inflow$FLOW/86400

inflow_start <- min(forecasted_inflow$time)
inflow_end <- max(forecasted_inflow$time)

## read in static datasets (edi, observation, etc.)

#chemistry
inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/199/12/a33a5283120c56e90ea414e76d5b7ddb" 
infile1 <- tempfile()
try(download.file(inUrl1,infile1,method="curl"))
if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")

BVRchem <- read.csv(infile1, header=T) |>
  select(Reservoir:DIC_mgL) |>
  dplyr::filter(Reservoir=="BVR") |>
  dplyr::filter(Site==100 | Site==200) |>
  mutate(DateTime = as.POSIXct(strptime(DateTime, "%Y-%m-%d", tz="EST"))) |>
  rename(time = DateTime)

#read in lab dataset of dissolved silica, measured by Jon in summer 2014 only
silica <- read.csv("./model_code/inflow_tmwb/inputs/FCR2014_Chemistry.csv", header=T) |>
  select(Date, Depth, DRSI_mgL) |>
  mutate(Date = as.POSIXct(strptime(Date, "%Y-%m-%d", tz="EST"))) |>
  dplyr::filter(Depth == 999) |> #999 = weir inflow site
  select(Date, DRSI_mgL) |>
  rename(time = Date)


#read in lab dataset of CH4 from 2015-2019
# for BVR: Only have a handful of days w/ CH4 in inflows (BVR 100 and 200); aggregate all time points
# and average CH4 - use average as CH4 input for the entier year
ghg <- read.csv("./model_code/inflow_tmwb/inputs/BVR_GHG_Inflow_20200619.csv", header=T) |>
  dplyr::filter(Reservoir == "BVR") |>
  dplyr::filter(Depth_m == 100|Depth_m == 200) |> #weir inflow (depth col should be site)
  select(DateTime, ch4_umolL) |>
  mutate(DateTime = as.POSIXct(strptime(DateTime, "%d-%b-%y", tz="EST"))) |>
  rename(time = DateTime, CAR_ch4 = ch4_umolL)

#read in lab dataset of pH 
inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/198/13/e50a50d062ee73f4d85e4f20b360ce4f" 
infile1 <- tempfile()
try(download.file(inUrl1,infile1))

pH <- read.csv(infile1) |>
  dplyr::filter(Reservoir == "BVR") |> 
  dplyr::filter(Site == 50) |> #don't have inflow ph so just going to use median deephole ph
  select(DateTime, Depth_m, pH) |>
  drop_na() |> 
  mutate(DateTime = as.POSIXct(strptime(DateTime, "%Y-%m-%d", tz="EST"))) |>
  rename(time = DateTime,
         CAR_pH = pH)

# start loop
inflow_build <- data.frame()

ensembles <- unique(forecasted_inflow$ensemble)

for (i in ensembles){
  
  print(i)
  
  inflow <- forecasted_inflow |> 
    filter(ensemble == i)
    
  # Create nuts (randomly sampled from a normal distribution) for total inflow
  bvr_nuts <- as.data.frame(seq.Date(as.Date(inflow_start),as.Date(inflow_end), "days"))
  names(bvr_nuts)[1] <- "time"
  bvr_nuts$time<-as.POSIXct(strptime(bvr_nuts$time, "%Y-%m-%d", tz="EST"))
  
  nuts_row_num <- nrow(bvr_nuts)
  
  bvr_nuts <- bvr_nuts |> 
    mutate(TN_ugL = rnorm(nuts_row_num,mean=mean(BVRchem$TN_ugL,na.rm=T),sd=sd(BVRchem$TN_ugL,na.rm=T))) |> 
    mutate(TP_ugL = rnorm(nuts_row_num,mean=mean(BVRchem$TP_ugL,na.rm=T),sd=sd(BVRchem$TP_ugL,na.rm=T))) |> 
    mutate(NH4_ugL = rnorm(nuts_row_num,mean=mean(BVRchem$NH4_ugL,na.rm=T),sd=sd(BVRchem$NH4_ugL,na.rm=T))) |> 
    mutate(NO3NO2_ugL = rnorm(nuts_row_num,mean=mean(BVRchem$NO3NO2_ugL,na.rm=T),sd=sd(BVRchem$NO3NO2_ugL,na.rm=T))) |> 
    mutate(SRP_ugL = rnorm(nuts_row_num,mean=mean(BVRchem$SRP_ugL,na.rm=T),sd=sd(BVRchem$SRP_ugL,na.rm=T))) |> 
    mutate(DOC_mgL = rnorm(nuts_row_num,mean=mean(BVRchem$DOC_mgL,na.rm=T),sd=sd(BVRchem$DOC_mgL,na.rm=T))) |> 
    mutate(DIC_mgL = rnorm(nuts_row_num,mean=mean(BVRchem$DIC_mgL,na.rm=T),sd=sd(BVRchem$DIC_mgL,na.rm=T)))
  
  # Make sure values are not negative!
  bvr_nuts <- bvr_nuts |> 
    mutate(TN_ugL = ifelse(TN_ugL<=0.00, 0.00, TN_ugL)) |> 
    mutate(TP_ugL = ifelse(TP_ugL<=0.00, 0.00, TP_ugL)) |> 
    mutate(NH4_ugL = ifelse(NH4_ugL<=0.00, 0.00, NH4_ugL)) |> 
    mutate(NO3NO2_ugL = ifelse(NO3NO2_ugL<=0.00, 0.00, NO3NO2_ugL)) |> 
    mutate(SRP_ugL = ifelse(SRP_ugL<=0.00, 0.00, SRP_ugL)) |>
    mutate(DOC_mgL = ifelse(DOC_mgL<=0.00, 0.00, DOC_mgL)) |> 
    mutate(DIC_mgL = ifelse(DIC_mgL<=0.00, 0.00, DIC_mgL))
  
  
  alldata<-merge(inflow, bvr_nuts, by="time", all.x=TRUE)
  
  
  # Calculate average for the BVR data points and mutate column to alldata
  alldata <- alldata |> mutate(CAR_ch4 = mean(ghg$CAR_ch4))
  
  #need to convert mass observed data into mmol/m3 units for two pools of organic carbon
  total_inflow <- alldata |> 
    mutate(NIT_amm = NH4_ugL*1000*0.001*(1/18.04)) |> 
    mutate(NIT_nit = NO3NO2_ugL*1000*0.001*(1/62.00)) |> #as all NO2 is converted to NO3
    mutate(PHS_frp = SRP_ugL*1000*0.001*(1/94.9714)) |> 
    mutate(OGM_doc = DOC_mgL*1000*(1/12.01)* 0.10) |> #assuming 10% of total DOC is in labile DOC pool (Wetzel page 753)
    mutate(OGM_docr = DOC_mgL*1000*(1/12.01)* 0.90) |> #assuming 90% of total DOC is in labile DOC pool
    mutate(TN_ugL = TN_ugL*1000*0.001*(1/14)) |> 
    mutate(TP_ugL = TP_ugL*1000*0.001*(1/30.97)) |> 
    mutate(OGM_poc = 0.1*(OGM_doc+OGM_docr)) |> #assuming that 10% of DOC is POC (Wetzel page 755)
    mutate(OGM_don = (5/6)*(TN_ugL-(NIT_amm+NIT_nit))*0.10) |> #DON is ~5x greater than PON (Wetzel page 220)
    mutate(OGM_donr = (5/6)*(TN_ugL-(NIT_amm+NIT_nit))*0.90) |> #to keep mass balance with DOC, DONr is 90% of total DON
    mutate(OGM_pon = (1/6)*(TN_ugL-(NIT_amm+NIT_nit))) |>
    mutate(OGM_dop = 0.3*(TP_ugL-PHS_frp)*0.10) |> #Wetzel page 241, 70% of total organic P = particulate organic; 30% = dissolved organic P
    mutate(OGM_dopr = 0.3*(TP_ugL-PHS_frp)*0.90) |> #Wetzel page 241, 70% of total organic P = particulate organic; 30% = dissolved organic P
    mutate(OGM_pop = 0.7*(TP_ugL-PHS_frp)) |> 
    #mutate(PHS_frp_ads = PHS_frp) |> #Following Farrell et al. 2020 EcolMod
    mutate(CAR_dic = DIC_mgL*1000*(1/52.515)) |> #Long-term avg pH of FCR is 6.5, at which point CO2/HCO3 is about 50-50
    mutate(CAR_pH = median(pH$CAR_pH))  |>
    mutate(SIL_rsi = median(silica$DRSI_mgL)) |>
    mutate(PHY_cyano = 0) |>
    mutate(PHY_cyano_IN = 0) |>
    mutate(PHY_cyano_IP = 0) |>
    mutate(PHY_green = 0) |>
    mutate(PHY_green_IN = 0) |>
    mutate(PHY_green_IP = 0) |>
    mutate(PHY_diatom = 0) |>
    mutate(PHY_diatom_IN = 0) |>
    mutate(PHY_diatom_IP = 0)
  #given this disparity, using a 50-50 weighted molecular weight (44.01 g/mol and 61.02 g/mol, respectively)
  #note: we are not using a DONr recalcitrant pool for inflows because "bacterial utilization of these 
  #compounds [i.e. DON] is extremely rapid" Wetzel p. 220
  #because we have added the pool of PHS_frp_ads, which functionally is DOPr, not adding a DOPr pool
  
  #reality check of mass balance: these histograms should be at zero minus rounding errors
  # hist(total_inflow$TP_ugL - (total_inflow$PHS_frp + total_inflow$OGM_dop + total_inflow$OGM_pop))
  # hist(total_inflow$TN_ugL - (total_inflow$NIT_amm + total_inflow$NIT_nit + total_inflow$OGM_don + total_inflow$OGM_pon))
  
  #creating OXY_oxy column using RMR package, assuming that oxygen is at 100% saturation in this very well-mixed stream
  # Obtained elevation from BVR DEM at BVR 100 inflow to the reservoir
  for(i in 1:length(total_inflow$TEMP)){
    total_inflow$OXY_oxy[i]<-(temp.C= Eq.Ox.conc(total_inflow$TEMP[i], elevation.m = 586,
                                                 bar.press = NULL, bar.units = NULL,
                                                 out.DO.meas = "mg/L",
                                                 salinity = 0, salinity.units = "pp.thou"))
    
    ## THE FOLLOWING METHOD PRODUCES VERY HIGH DO VALUES (AUSTIN REMOVED 1000*(1/32) CONVERSION)
    # total_inflow$OXY_oxy[i]<-(temp.C= Eq.Ox.conc(total_inflow$TEMP[i], elevation.m = 586,
    #                                              bar.press = NULL, bar.units = NULL,
    #                                              out.DO.meas = "mg/L",
    #                                              salinity = 0, salinity.units = "pp.thou"))*1000*(1/32)
  }
  
  #clean it up and get vars in order
  total_inflow <- total_inflow |>
    #select(time, FLOW, TEMP, SALT, ensemble, OXY_oxy, NIT_amm:SIL_rsi, CAR_ch4, PHY_cyano:PHY_diatom_IP) |> 
    mutate(SIL_rsi = SIL_rsi*1000*(1/60.08) * 0.65) |> #setting the Silica concentration to the median 2014 inflow concentration for consistency
    #note scaling silica down bc too high!!
    # mutate(NIT_nit = NIT_nit * 0.2) |> #scaling nitrate inflow by 0.2 bc too high
    # mutate(NIT_amm = NIT_amm * 0.4) |> #scaling ammonium inflow by 0.4 bc too high
    # mutate(OGM_docr = OGM_docr * 1.9) |> #docr too low
    # mutate(OGM_doc = OGM_doc * 1.7) |> #bc docr:doc needs to be 10:1
    mutate_if(is.numeric, round, 4) #round to 4 digits 
  
  #note that labile doc is higher in more eutrophic systems - maybe 20% as an avg, but could be a greater % of TOC pool
  #wetzel says that ratio in reservoirs w/ high allochthonous loading could be 4:1
  
  #estimate bvr inflow temp based on relationship between bvr and fcr inflows
  total_inflow$TEMP <- (1.5 * total_inflow$TEMP) - 9.21
  
  #make sure time is 12:00
  total_inflow$time <- total_inflow$time +  hours(12) + minutes(00) + seconds(00)
  
  #scale inflow so that water level AND wrt look reasonable
  #total_inflow$FLOW <- total_inflow$FLOW 
  
  inflow_build <- dplyr::bind_rows(inflow_build, total_inflow)


} # end for loop

return(inflow_build)

} # end function




############################################################################


#save scaled file
#write.csv(inflow_build, "./model_code/inflow_tmwb/inputs/model_output/inflow_tmwb/inflow_forecast_all_vars.csv", row.names = F)





############# DO WE NEED TO DO THE OUTFLOW PORTION BELOW ?? ##########################



##############################################################
##############################################################

#REGARDLESS OF YOUR OC POOLS, WILL NEED TO MAKE OUTFLOW!
# For BVR, need to take into account changing water level as well

# Load in water level + volume data for BVR
# Calculated in WaterLevel_BVR.Rmd (inputs/water_level)
# vol <- read_csv("./model_code/inflow_tmwb/inputs/inputs/BVR_Daily_WaterLevel_Vol_2015_2022_interp.csv")
# vol$Date <- as.POSIXct(strptime(vol$Date, "%Y-%m-%d", tz = "EST"))
# 
# vol1 <- vol |> filter(Date>=as.Date('2015-07-07')&Date<=as.Date('2022-05-03')) |> select(Date,vol_m3)
# vol2 <- vol |> filter(Date>=as.Date('2015-07-08')&Date<=as.Date('2022-05-04')) |> select(Date,vol_m3)
# 
# dvol <- vol |> filter(Date>=as.Date('2015-07-07')&Date<=as.Date('2022-05-03')) |> select(Date)
# 
# # Calculate dVol/dt by vol2-vol1/s
# vol3 <- as.data.frame((vol2$vol_m3 - vol1$vol_m3)/(24*60*60))
# names(vol3)[1] <- "dv_m3s"
# 
# dvol <- cbind.data.frame(dvol,vol3)
# alldata<-merge(inflow, bvr_nuts, by="time", all.x=TRUE)
# 
# # Check change in water level
# ggplot(dvol,mapping=aes(x=Date,y=dv_m3s))+
#   geom_line()
# 
# #drop rows that are NA in alldata
# alldata <- alldata[!(is.na(alldata$FLOW)),]
# 
# #drop last date so dfs line up
# alldata <- alldata[c(1:2493),]
# 
# #check to make sure dates line up
# df <- data.frame(one = alldata$time, two=dvol$Date) #had to drop 4 dates because missing between the 2 dfs
# 
# # Calculate outflow as the total inflow - change in volume 
# outflow <- as.data.frame(alldata$FLOW-dvol$dv_m3s) 
# names(outflow)[1] <- "FLOW"
# outflow <- cbind.data.frame(dvol,outflow)
# outflow <- outflow |> select(Date,FLOW) |> 
#   mutate_if(is.numeric,round,4) #round to 4 digits
# names(outflow)[1] <- "time"
# 
# 
# #diagnostic plot
# ggplot()+
#   #geom_line(outflow,mapping=aes(x=as.Date(time),y=FLOW,color="Outflow"))+
#   geom_line(inflow,mapping=aes(x=as.Date(time),y=FLOW,color="Inflow"))+
#   theme_classic(base_size=15)
# 
# #make sure time is 12:00
# outflow$time <- outflow$time +  hours(12) + minutes(00) + seconds(00)
# 
# #set negative and 0 flow = 0.0001
# outflow$FLOW[outflow$FLOW<=0] <- 0.0001
# 
# #write file
# write.csv(outflow, "./BVR_spillway_outflow_2015_2022_metInflow.csv", row.names=F)
# 
# 
# 
# # #read in fcr inflow
# # inUrl1  <- "https://pasta.lternet.edu/package/data/eml/edi/202/9/c065ff822e73c747f378efe47f5af12b" 
# # infile1 <- tempfile()
# # try(download.file(inUrl1,infile1))
# # if (is.na(file.size(infile1))) download.file(inUrl1,infile1,method="auto")
# # 
# # temp <- read.csv(infile1)
# # 
# # # plot fcr weir temp vs. fcr air temp + find relationship
# # fcr_weir  <- temp |>
# #   select(DateTime, WVWA_Temp_C) |>
# #   mutate(DateTime = as.Date(DateTime)) |>
# #   group_by(DateTime) |>
# #   summarise(inflow_temp = mean(WVWA_Temp_C, na.rm=T)) |>
# #   filter(DateTime >= "2015-07-07" & DateTime <= "2022-05-05")
# # 
# # fcr_air <- read_csv("sims/baseline/inputs/met.csv") |>
# #   select(time, AirTemp) |>
# #   mutate(DateTime = as.Date(time)) |>
# #   group_by(DateTime) |>
# #   summarise(airtemp = mean(AirTemp, na.rm=T)) |>
# #   filter(DateTime %in% fcr_weir$DateTime)
# # 
# # # plot(fcr_weir$inflow_temp ~ fcr_air$airtemp)
# # # abline(a = 0, b = 1, col = "red", lwd = 2, lty = 2)
# # 
# # # Fit a linear model
# # model <- lm(fcr_weir$inflow_temp ~ fcr_air$airtemp)
# # 
# # # Extract slope and intercept
# # slope <- coef(model)[2]
# # intercept <- coef(model)[1]
# # 
# # # Print equation
# # # equation <- paste0("y = ", round(intercept, 2), " + ", round(slope, 2), "x")
# # # print(equation)
