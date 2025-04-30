Barom.Press <-
  function(elevation.m, units = "atm"){
    if(units == "atm") {fact <-1}
    else if(units == "kpa") {
      fact <- 101.325
    }else if(units == "mmHg") {
      fact <- 760
    }else{
      stop("invalid pressure units, must be
                 'atm', 'kpa', or 'mmHg'")
    }
    P<-exp(-9.80665 * 0.0289644 * elevation.m / 
             (8.31447 * 288.15))*fact
    return(P)
  }

Eq.Ox.conc <- function(temp.C, elevation.m = NULL,
           bar.press = NULL, bar.units = "mmHg",
           out.DO.meas = "mg/L", salinity = 0,
           salinity.units = "pp.thou"){
    tk <- 273.15 + temp.C
    if(is.null(elevation.m) == FALSE &&
       is.null(bar.press) == FALSE){
      stop("'bar.press' must be NULL if 'elevation.m' is assigned a value. ")
    }
    
    if( out.DO.meas == "PP"){
      
      if(is.null(bar.press) == FALSE && is.null(elevation.m) == TRUE){
        bar.press <- bar.press
      } else if(is.null(bar.press) == TRUE &&
                is.null(elevation.m) == FALSE){
        bar.press <- Barom.Press(elevation.m,
                                  units = bar.units)
      }else{
        stop("EITHER 'elevation.m' or 'bar.press' must be assigned
                     a value. The other argument must be NULL.")
      }
      if(bar.units == "atm"){
        bar.press.atm <- bar.press
      }else if(bar.units == "kpa"){
        bar.press.atm <- bar.press / 101.32501
      }else if(bar.units == "mmHg"){
        bar.press.atm <- bar.press / 760
      }else{
        stop("invalid pressure units, must be
                     'atm', 'kpa', or 'mmHg'")
      }
      DO <- bar.press*0.20946
      
      
    }else if (out.DO.meas == "mg/L"){
      
      if(is.null(bar.press) == FALSE &&
         is.null(elevation.m) == TRUE){
        if(bar.units == "atm"){
          bar.press.atm <- bar.press
        }else if(bar.units == "kpa"){
          bar.press.atm <- bar.press / 101.32501
        }else if(bar.units == "mmHg"){
          bar.press.atm <- bar.press / 760
        }else{
          stop("invalid pressure units, must be
                             'atm', 'kpa', or 'mmHg'")
        }
      } else if(is.null(bar.press) == TRUE &&
                is.null(elevation.m) == FALSE){
        bar.press.atm <- Barom.Press (elevation.m, units = "atm")
      }else{
        stop("EITHER 'elevation.m' or 'barom.press' must be assigned
                             a value. The other argument must be NULL.")
      }
      ## Benson and Krause eqns, USGS 2011 ##
      
      A1 <- -139.34411
      A2 <- 1.575701e5
      A3 <- 6.642308e7
      A4 <- 1.243800e10
      A5 <- 8.621949e11
      
      DO <- exp(A1 + (A2/tk) -
                  (A3/(tk^2)) +
                  (A4/(tk^3)) -
                  (A5/(tk^4)))
      
    }else{
      stop("must specify 'out.DO.meas' as 'mg/L' or 'PP'")
    }
    # salinity factor #
    if(salinity.units == "uS"){
      sal <- salinity*5.572e-4 + (salinity^2)*2.02e-9 
    }else if(salinity.units == "pp.thou"){
      sal <- salinity
    }else{
      stop("salinity.units must be 'uS' or 'pp.thou'")
    }
    
    Fs <- exp(-sal*(0.017674 - (10.754/tk) + (2140.7/(tk^2))))
    
    ## Pressure factor determination ##
    theta <- 0.000975 - 
      temp.C*1.426e-5 +
      (temp.C^2)*6.436e-8
    
    u <- exp(11.8571 - (3840.70/tk) - (216961/(tk^2)))
    
    # pressure factor #
    
    Fp <- ((bar.press.atm - u)*(1-(theta*bar.press.atm))) /
      ((1-u)*(1-theta))
    
    Cp <- DO * Fs * Fp
    
    return(Cp)
  }
