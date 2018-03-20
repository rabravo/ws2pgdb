#' ws_metadata_2_pgdb() stores metadata of NOAA info into a local or remote database server
#'
#' This function works in cascade with the all_coor_ws(). This function uses the 
#' mindate and maxdate returned from the rnoaa API data frame. Weather stations are described 
#' in terms of mindate, and maxdate dates, were weather data is available. 
#' Hence, given a particular variable (i.e. TMAX, TMIN, PRCP, etc), we are interested 
#' to retrieve the number of weather stations whose time window defined by the 
#' interval [mindate, maxdate] intersects and the length of the time window is maximum. 
#' Searching for answers online, it seems that this is a hard computing problem. 
#' It belongs to the set of problems defined as the 'Maximum Subset Intersection' problem 
#' which are close-related to the set cover problem studied in graph theory. The following 
#' function computes a resulting set using an heuristic described through the comments 
#' in the body of the function.  
#'
#' @param geoid  FIPS number from census tables (tiger files)
#' @param type   Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param stations The universe of stations from the rnoaa API from where the new subset will be computed.
#' @return function returns the name of the new table of the subset of weather stations with intersecting data 
#' @examples
#' ghcnd    <- 'GHCND'
#' geoid    <- '12087'
#' type     <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' ws_metadata_2_pgdb( geoid, type, stations ) 
#' @note Remember that all_coor_ws() returns a set of stations.
#' @export
ws_metadata_2_pgdb <- function( geoid, type, stations){

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  drv     <- "PostgreSQL"
  conn	  <- RPostgreSQL::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if ( as.integer(geoid) < 100) {
  
    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn, q1)
    state <- data.frame(RPostgreSQL::fetch(res))
    RPostgreSQL::dbClearResult(res)
    tableName <- base::paste(state, "_", geoid,"_ws_metadata_", sep="")

  } else {

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn, q2)
    county<- data.frame(RPostgreSQL::fetch(res))
    RPostgreSQL::dbClearResult(res)
    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn,q3)
    state <- data.frame(RPostgreSQL::fetch(res))
    RPostgreSQL::dbClearResult(res)
    tableName<- base::paste(state, "_",county,"_",geoid,"_ws_metadata_", sep="")

  }# endIF/ELSE

  varTable  <- tolower( tableName  )
  type      <- tolower(  type  )
  tableName <- base::paste( varTable, type, sep="")      
  tableName <- gsub(" ", "_", tableName)
  
  if (RPostgreSQL::dbExistsTable(conn, tableName)) {

    msg <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")
    cat(msg)
    RPostgreSQL::dbDisconnect(conn)
    return(tableName)    

  } else {

    name_and_date.df<- stations[,c("id","mindate","maxdate","longitude","latitude")]

    # Sorted Indexes
    order.maxdate   <- order(name_and_date.df$maxdate, decreasing = TRUE)   	

    # Get the intervals sort in decreasing order      
    stationSortByMaxDate <- name_and_date.df[order.maxdate,]                 	

    # Filter station with datacoverage > 90%
    # subIntervalDataCover      <- subset( stationSortByMaxDate, datacoverage >= 0.90 )

    # Filter station with maxdate year in 2015
    subIntervalYear <- subset( stationSortByMaxDate, lubridate::year(  maxdate ) == lubridate::year( lubridate::today() ) )

    # Update date to beginning of current year  
    subIntervalYear$maxdate <- base::as.Date( lubridate::floor_date( lubridate::today() , "year") )  	

    # Sorted Indexes
    order.mindate   <- order( subIntervalYear$mindate, decreasing = TRUE)    

    # Get the intervals sort in decreasing order
    stationSortByMinDate <- subIntervalYear[order.mindate,]                  	

  
    # Guarantees an intersection of data of all weather stations from current year back to
    # a threshold of years. (i.e. current year 2015, and threshold equal 10 or 2005)
    threshold   <- 10

    # When the number of available stations is LESS than numStations, code chooses all stations; 
    # otherwise, it uses the REPEAT iteration to gather at least five stations. 
    numStations <- 5
  
   
    if ( !( isTRUE( nrow( stationSortByMinDate ) < numStations ) ) ) {

      # Theshold guarantees latest information    
      thrs <- lubridate::year( base::as.Date( lubridate::floor_date(  lubridate::today() , "year") ) ) - threshold
    
      subIntervalMinSizeOfAYear <- subset(stationSortByMinDate,  lubridate::year( mindate ) <= thrs ) 
    
      # Heursitics: when number of weather stations is small, this section will intend to increase the 
      # number of weather stations while reducing the threshold value.
    
      repeat{
    
        # The number of minimum weather stations     
        if ( !( isTRUE( nrow(subIntervalMinSizeOfAYear) >= numStations - 1 ) ) ) {
          thrs <- thrs - 1
          subIntervalMinSizeOfAYear <- subset(stationSortByMinDate,  lubridate::year( mindate  ) <= thrs ) 
        } else { break }# endIF/ELSE      
      }# endREPEAT
    
    } else { 
         order.min <- order( stationSortByMinDate$mindate, decreasing = TRUE)
         subIntervalMinSizeOfAYear <- stationSortByMinDate[order.min,]
	 # subIntervalMinSizeOfAYear <- stationSortByMinDate
    }# endIF/ELSE

    # Extract the sorted element with the earliest start time  
    startDate <- subIntervalMinSizeOfAYear$mindate[1]                     
    
    # Update dates to startDate value to all elements in the array
    subIntervalMinSizeOfAYear$mindate <- base::as.Date(startDate)

    # Simplistic naming return this data structure
    station.df <- subIntervalMinSizeOfAYear                                  
    
    msg <- base::paste("Creating ", tableName, " table of ", type , sep="")    
    Sys.sleep(3)
    cat(msg)

    pts   <- as.data.frame( station.df[,c("longitude","latitude")] )
    coord <- sp::SpatialPoints(pts)
    spdf  <- sp::SpatialPointsDataFrame(coord, pts)
    proj  <- "+init=epsg:4269"
    sp::proj4string(spdf) <- proj #CRS(proj)

    # Insert attributes into the SpatialPointsDataFrame 
    spdf$name	 <- station.df$id
    spdf$mindate <- station.df$mindate
    spdf$maxdate <- station.df$maxdate
    spdf$ogc_fid <- seq.int(nrow(station))
    #spdf         <- tibble::rowid_to_column(spdf,"ogc_fid") #Need it for historical reasons  

    rpostgis::pgInsert(conn, name = c("public", tableName), data.obj = spdf, geom = "geom")  
     
    cat("Finished. Check Postgres table")
  
    station.df <- spdf 
   
  }# endIF/ELSE
  
  RPostgreSQL::dbDisconnect(conn)
  RPostgreSQL::dbUnloadDriver(drv)      
  return(tableName)

}# endFUNCTION
