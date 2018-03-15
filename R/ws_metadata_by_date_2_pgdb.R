#' ws_metadata_by_date_2_pgdb() stores NOAA information into local or remote database server
#'
#' This function uses the metadata returned from all_coor_ws which in turn uses the rnoaa API 
#' to retrieve NOAA data. Metadata includes the mindate, and maxdate dates. 
#' Given a particular variable (i.e. TMAX, TMIN, PRCP, etc), we are interested 
#' to retrieve the number of weather stations whose time window defined by the 
#' interval [mindate, maxdate] intersects and the length of the time window is maximum. 
#' Searching for answers online,the solution is a hard computing problem. 
#' It belongs to the set of problems defined as the 'Maximum Subset Intersection' problem 
#' which are close-related to the set cover problem studied in graph theory. The following 
#' function computes a resulting set using an heuristic described through the comments 
#' in the body of the function. It starts with the current year and the span variable defines
#' the threshold for the algorithm to search for the answers. 
#'
#' @param geoid  FIPS number from census tables (tiger files)
#' @param type   Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param stations The universe of stations from the rnoaa API from where the new subset will be computed.
#' @param span This is the threshold used to limit the search .
#' @return Returns the name of the new table of the subset of weather stations with intersecting data 
#' @examples
#' ghcnd    <- 'GHCND'
#' geoid    <- '48113'
#' type     <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' ssDate   <- '2000-01-01'
#' ffDate   <- '2001-01-01'
#' ws_metadata_by_date_2_pgdb( geoid, type, stations, ssDate, ffDate) 
#' @note Remember that all_coor_ws() returns a set of stations.
#' @export
ws_metadata_by_date_2_pgdb <- function( geoid, type, stations, ssDate, ffDate){

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  drv <- "PostgreSQL"
  conn   <- RPostgreSQL::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  sDate  <- gsub("-", "", ssDate)
  fDate  <- gsub("-", "", ffDate)

  if ( as.integer(geoid) < 100) {
  
    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn, q1)
    state <- RPostgreSQL::fetch(res)
    RPostgreSQL::dbClearResult(res)
    tableName <- base::paste(state, "_", geoid,"_ws_metadata_from_",sDate,"_to_",fDate, sep="")

  } else {

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn, q2)
    county<- RPostgreSQL::fetch(res)
    RPostgreSQL::dbClearResult(res)

    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgreSQL::dbSendQuery(conn, q3)
    state <- RPostgreSQL::fetch(res)
    RPostgreSQL::dbClearResult(res)
    tableName<- base::paste(state, "_",county,"_",geoid,"_ws_metadata_from_",sDate,"_to_",fDate, sep="")

  }# endIF/ELSE

  varTable  <- tolower( tableName  )
  type      <- tolower(  type  )
  tableName <- base::paste( varTable,"_", type, sep="")      
  tableName <- gsub(" ", "_", tableName)

  if (RPostgres::dbExistsTable(conn, tableName)) {

    msg <- base::paste("\nDone - Table ", tableName, " exists.\t\t\t\t\t\n", sep="")
    Sys.sleep(3)    
    cat(msg)
    RPostgreSQL::dbDisconnect(conn)
    RPostgreSQL::dbUnloadDriver(drv)
    return(tableName)    

  } else {

    df<- stations[,c("id","mindate","maxdate","longitude","latitude")]
    

    # Keep stations with maxdate year greater or equal to ffDate year 
    subMaxIntervalYear <- subset(df, (lubridate::year(as.Date(maxdate)) >= lubridate::year(base::as.Date(ffDate))))

    # Update date to the beginning of ffDate year  
    subMaxIntervalYear$maxdate <- base::as.Date( lubridate::ymd( ffDate ) )  	
    
    # Keep stations with mindate year less or equal to ssDate year
    subMinIntervalYear <- subset(subMaxIntervalYear, (lubridate::year(as.Date(mindate)) <= lubridate::year(base::as.Date(ssDate))))

    # print(subMinIntervalYear)
    startDate <- base::as.Date( lubridate::ymd( ssDate ) )

    # Update dates to startDate value to all elements in the array
    subMinIntervalYear$mindate <- base::as.Date(startDate)
  
    # Simplistic naming return this data structure
    station.df <- subMinIntervalYear                                  
    
    msg <- base::paste("Creating ", tableName, " table of ", type, "\n",sep="")    
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
   
    rpostgis::pgInsert(conn, name = c("public", tableName), data.obj = spdf, geom = "geom")
   
    cat("Finished. Check Postgres table\n")
  
    station.df <- spdf 
   
  }# endIF/ELSE
  
  RPostgreSQL::dbDisconnect(conn)
  RPostgreSQL::dbUnloadDriver(drv)

  return(tableName)
}# endFUNCTION
