#' ws_data_avg_span_2_pgdb() retrieves the subset of ws with intersectin data
#'
#' Same as ws_data_na_2_pgdb() , retrieves the weather data from the intersecting stations. 
#' It is known that NOAA enlist available data for certain years, months, or days; however, 
#' some of this information is missing. This function replaces the missing data with the average value of the dataset. 
#' The example shows the use of ws_metadata_span_2_pgdb(), but it can be very well be replaced by ws_metadata_2_pgdb().  
#'
#' @keywords NOAA, weather station, rgdal, RPostgreSQL, rnoaa
#' @param ghcnd    String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid    FIPS number from census tables
#' @param type     Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param span	   Indicates look-back time to search for available ws stations and data
#' @param ws_metadata  The table name of the subset of weather stations with intersecting data
#' @return Returns the name of the table that has been created. When fail, it returns 1.

#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' ws_metadata  <- ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
#' ws_data_avg_span_2_pgdb( ghcnd, geoid, type, span, ws_metadata)
#'
#' @export
ws_data_avg_span_2_pgdb <- function( ghcnd, geoid, type, span, ws_metadata){

  #FIPS <- base::paste( "FIPS:", geoid, sep="")
  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  
  if( as.integer(geoid) < 100){  
    q1     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q1)
    state  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",geoid,"_ws_data_span_",span,"_avg_", sep="")

  }else{

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    state  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_data_span_",span,"_avg_", sep="")
  }

  type      <- base::tolower(  type  )
  tableName <- base::gsub(" ", "_", tableName)
  tableName <- base::tolower( tableName ) 
  tableName <- base::paste( tableName, type, sep="")      
  #print( tableName )

  if(config$isgraphic){

    w    <- gWidgets::gwindow("Message: ", width=500)
    gp   <- gWidgets::ggroup(container=w, expand=TRUE)
    txt  <- gWidgets::glabel("Creating Postgres weather information table \t\t\t\t", expand=TRUE, container=gp)

  }else{ cat("Creating Postgres weather information table \t\t\t\t\n")}
  
  if(RPostgres::dbExistsTable(conn, tableName)){

    msg <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")

    if(config$isgraphic){

      gWidgets::svalue(txt) <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\t", sep="")
      Sys.sleep(5)    
      gWidgets::gmessage("Check Postgres table!\n")

    } else { 

      cat(msg)
      cat("\nCheck Postgres table!")

    }

    RPostgres::dbDisconnect(conn)
    return(tableName)
    
  } else {
	  
          q1 <- base::paste("select name as id, mindate as mindate, maxdate as maxdate, longitude as longitude, latitude as latitude from ", ws_metadata, sep="")
          res <- RPostgres::dbSendQuery(conn, q1)
          station <- data.frame(RPostgres::dbFetch(res))
          RPostgres::dbClearResult(res)
	  minDate <- base::as.Date( lubridate::ymd( station$mindate[1] ) )
	  maxDate <- base::as.Date( lubridate::ymd( station$maxdate[1] ) )
	  #One is end in a new year.  
	  ndays <- ( lubridate::int_length( lubridate::interval( base::as.Date( minDate ) , base::as.Date( maxDate )))/(3600*24))+1 
	  dates <- as.character( seq.Date( minDate , by ="days", length.out= ndays) )
	
	  #The NOAA API only allows queries from users to retrieve data sets by year; 
	  #thus, this code section limits each query to a different year for each iteration.  
	  #WARNING: There is 500 limit request per day with the current token 
	
	  for(i in 1:nrow( station ) ){  
	
	    ws	 <- station$id[i]
	    #startDate
	    sDate	 <- minDate
	    #finishDate                 
	    fDate	 <- minDate

  	    #lubridate::year( sDate )	<- lubridate::year( maxDate ) - 9   
	    #lubridate::year( fDate )	<- lubridate::year( maxDate ) - 9
  
	    lubridate::year( fDate ) <- lubridate::year( fDate ) + 1    #Interval (sDate, fDate) is one year.

	    #Temporal strings attached to the dates for consistency with NOAA 	    
	    ssDate <- base::paste(sDate, "T00:00:00", sep="")
	    ffDate <- base::paste(fDate, "T00:00:00", sep="")
            estacion       <- base::paste("GHCND:", ws, sep="")


	    weatherVar <- rnoaa::ncdc(datasetid=ghcnd, stationid=estacion, datatypeid=type, startdate=ssDate, enddate=ffDate , limit=366, token=config$token)
	    #Verify that available information exist
            if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
              if ( config$isgraphic ){
                gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion," with variable type:  ", type, " exists.\t\t\t\t\n", sep="" )
              }else{
                msg <- base::paste("Not Data Available for station ", estacion ," with variable type:  ", type, " exists.\t\t\t\t\n", sep="" )
                cat( msg )
                print("Spotted a Null\n")
              }
	      colnames( valores ) <- station$id[1:(i-1)]
              RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
              RPostgres::dbDisconnect( conn )
              return(tableName) 
            }

	    weatherYear<- weatherVar$data  
 
            #if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
            #  print("Spotted a Null\n")
	    #  colnames( valores ) <- station$id
            #  RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
            #  RPostgres::dbDisconnect( conn )
            #  return(tableName) 
            #}

    
	    repeat
	    {              

	      #Replaces starting year, year( sDate ), with a value of new time series
	      lubridate::year( sDate )  <- lubridate::year( sDate )  + 1

	      #Replaces starting day, day( sDate ), with a value of new time
	      lubridate::day(  sDate )  <- lubridate::day(  fDate )  + 1    

	      #Replaces ending year, year( fDate ), with a value of new time series
	      lubridate::year( fDate )  <- lubridate::year( fDate )  + 1

	      ssDate         <- base::paste(sDate, "T00:00:00", sep="")
	      ffDate         <- base::paste(fDate, "T00:00:00", sep="")


	      if( isTRUE( lubridate::year( fDate ) < lubridate::year( maxDate ) ) )
	      { 
	 
	        weatherVar<-rnoaa::ncdc(datasetid = ghcnd, stationid = estacion, datatypeid=type, startdate = ssDate, enddate= ffDate , limit=366, token=config$token)

		#Verify that available information exist 
                if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
                  if ( config$isgraphic ){
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion," with variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, " exists.\t\n", sep="" )
                    cat( msg )
                  }
                
	          colnames( valores ) <- station$id[1:(i-1)]
                  RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
                  RPostgres::dbDisconnect( conn )
                  return(tableName) 
                }

		#Delay needed since NOAA service accepts a maximum of 5 request per second.
		Sys.sleep(0.2)											   	    
	        weatherYear <- rbind(weatherYear, weatherVar$data)
	 
	      }
	      else
	      { 
	      
	        lubridate::month( fDate ) <- lubridate::month( maxDate ) 
	        lubridate::day(   fDate ) <- lubridate::day(   maxDate ) 
	        ssDate         <- base::paste( sDate, "T00:00:00", sep="")
	        ffDate         <- base::paste( fDate, "T00:00:00", sep="")                      
	      
	        weatherVar<-rnoaa::ncdc(datasetid = ghcnd, stationid = estacion, datatypeid=type, startdate = ssDate, enddate= ffDate , limit=366, token=config$token)


		#Verify that available information exist 
                if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
                  if ( config$isgraphic ){
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion," with variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, " exists.\t\n", sep="" )
                    cat( msg )
                  }
	          colnames( valores ) <- station$id[1:(i-1)]
                  RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
                  RPostgres::dbDisconnect( conn )
                  return(tableName) 
                }

	        weatherYear    <- rbind( weatherYear, weatherVar$data)      
	        break
	      }
	      
	    }#End cycle to gather information for one weather station


	    # TMAX = Maximum temperature (tenths of degrees C)	    
	    promedio <- sprintf( "%.4f", mean( weatherYear$value/10 ) )     
	    
	    #NOAA specification of available data within a range often is not complete. 
	    #Individual dates are missing even within an specified range.
	    #SOLUTIONS:  
	      #1) Average from the available number of samples.  
	      #2) Interpolation when possible

	
	    if ( i == 1)
	    {	    
	      intervalAsDate     <- base::as.Date( weatherYear$date )
              # TMAX = Maximum temperature (tenths of degrees C)
	      intervalAsValue    <- sprintf("%.4f", weatherYear$value/10)  
	      stationDataHash    <- hash::hash( intervalAsDate , intervalAsValue )                
	      allDatesHash       <- hash::hash( dates ,  rep(1, ndays ) )         # h1 <- hash( dates , rep(1, ndays ) )   
	      hash::values( allDatesHash, keys=intervalAsDate ) <- intervalAsValue
	      v1.df              <- as.data.frame( hash::has.key( dates , stationDataHash ) )
	      colnames(v1.df)    <- "value"
	      
	      #Section that guarantees that v1.df has at least one value
	      if( length( which( v1.df$value == FALSE) ) > 0  )
	      {            
	      
	        sub_v1.df       <- as.data.frame( subset( v1.df, value == FALSE ) )

		# Dates from station which values do not exist in allDatasHash
	        dateKeys        <- as.character( rownames( sub_v1.df ) )  #print(dateKeys)
	        sizeSubV1       <- nrow( sub_v1.df )
	        
	        #In v2 the vecPromedio variable hold the average value of the available data.
	        vecPromedio     <- rep( promedio, sizeSubV1  )	
	        
		#Update allDatesHash values found in 'dataKeys' with average 
	        hash::values( allDatesHash, keys = dateKeys) <- vecPromedio
	        valores <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )
	      }else{  valores  <- as.data.frame( intervalAsValue ) }#endIF/ELSE
	      
	    }
	    else
	    {
	    
	      intervalAsDate     <-  base::as.Date( weatherYear$date )

	      # TMAX = Maximum temperature (tenths of degrees C)
	      intervalAsValue    <- sprintf( "%.4f", weatherYear$value/10 ) 
	      stationDataHash    <- hash::hash( intervalAsDate , intervalAsValue )
	      hash::values( allDatesHash, keys=dates) <- rep(1, ndays )	                
	      hash::values( allDatesHash, keys=intervalAsDate) <- intervalAsValue
	      v1.df              <- as.data.frame( hash::has.key(  dates , stationDataHash ) ) 	      
	      colnames(v1.df)    <- "value"
	      
	      #Section guarantees that v1.df has at least one value
	      if( length( which( v1.df$value == FALSE) ) > 0  )
	      {            

	        sub_v1.df        <- as.data.frame( subset( v1.df, value == FALSE) )
	        dateKeys         <- as.character( rownames( sub_v1.df ) )  #print(dateKeys)
	        sizeSubV1        <- nrow( sub_v1.df)
	        
	        vecPromedio      <- rep( promedio, sizeSubV1  )
	        hash::values( allDatesHash, keys=dateKeys) <- vecPromedio
	        subValores       <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )            
	        valores          <- as.data.frame( cbind( valores, subValores ) )	        
	      }else{ 
		valores     <- as.data.frame( cbind( valores, intervalAsValue ) ) 
              }#endIF/ELSE
	      
	    }
	        	    
            rm( stationDataHash )	
        
	  }#endFOR   (stick time series of different stations together)	
	  
	  colnames( valores ) <- station$id
      
      if (config$isgraphic){	
        w2   <- gWidgets::gwindow("Message: ", width=500)
        gp2  <- gWidgets::ggroup(container=w2, expand=TRUE)
        txt2 <- gWidgets::glabel("", expand=TRUE, container=gp2)	
        gWidgets::svalue( txt2 ) <- base::paste("Creating table ", tableName, ".\t\t\t\t\t", sep="")
      }else{
        msg <- base::paste("\nCreating table ", tableName, ".\t\t\t\t\t", sep="")
        cat(msg)
      }
      RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
      Sys.sleep(4)
      if(config$isgraphic){
        gWidgets::gmessage("Check Postgres table!\n")    
      }else{cat("Check Postgres table!\n")}
	  
  }#endIF/ELSE


  RPostgres::dbDisconnect( conn )
  return(tableName) 

}#end FUNCTION
