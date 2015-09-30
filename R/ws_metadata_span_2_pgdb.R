#' ws_metadata_span_2_pgdb() stores NOAA information into local or remote database server
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
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' @note Remember that all_coor_ws() returns a set of stations.
#' @export
ws_metadata_span_2_pgdb <- function( geoid, type, stations, span){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  if( as.integer(geoid) < 100){  
  
    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q1)
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_", geoid,"_ws_metadata_span_",span,"_", sep="")

  }else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q2)
    county<- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)

    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q3)
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName<- base::paste(state, "_",county,"_",geoid,"_ws_metadata_span_",span,"_", sep="")

  }

  varTable  <- tolower( tableName  )
  type      <- tolower(  type  )
  tableName <- base::paste( varTable, type, sep="")      
  tableName <- gsub(" ", "_", tableName)
  
  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("", expand=TRUE, container=gp)

  }

  if(RPostgres::dbExistsTable(conn, tableName)){


    msg <- base::paste("\nDone - Table ", tableName, " exists.\t\t\t\t\t\n", sep="")

    if ( config$isgraphic ){
      gWidgets::svalue(txt) <- msg
      #svalue(txt2) <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep="" )
      Sys.sleep(3)    
      gWidgets::gmessage("Check Postgresql table.\n")
    } else { cat(msg) }
    
    RPostgres::dbDisconnect(conn)
    #RPostgres::dbUnloadDriver(drv)
    return(tableName)    

  } else {

   df<- stations[,c("id","mindate","maxdate","longitude","latitude")]
    

    #Keep stations with maxdate year today's year 
    subMaxIntervalYear <- subset(df, (lubridate::year(as.Date(maxdate)) == lubridate::year(lubridate::today())) )

    #Update date to beginning of current year  
    subMaxIntervalYear$maxdate <- base::as.Date( lubridate::floor_date( lubridate::ymd( lubridate::today() ) , "year") )  	

    #print(lubridate::year(as.Date(subMaxIntervalYear$mindate)) <= (lubridate::year(lubridate::today()) - base::as.integer(span)))
    subMinIntervalYear <- subset(subMaxIntervalYear, (lubridate::year(as.Date(mindate)) <= (lubridate::year(lubridate::today()) - base::as.integer(span))))

#print(subMinIntervalYear)
    #mindate (span) set by user         
    startDate <- base::as.Date( lubridate::floor_date( lubridate::today() , "year") - lubridate::years( base::as.integer(span) ) )

    #Update dates to startDate value to all elements in the array
    subMinIntervalYear$mindate <- base::as.Date(startDate)
  
    #Simplistic naming return this data structure
    station.df <- subMinIntervalYear                                  
    
    msg <- base::paste("Creating ", tableName, " table of ", type, "\n",sep="")    
    if ( config$isgraphic ){

      gWidgets::svalue(txt) <- msg #"Creating table ...\t\t\t\t\t"
      Sys.sleep(3)

    }else{ cat(msg) }

    coord <- as.data.frame( station.df[,c("longitude","latitude")] )
    proj  <- "+init=epsg:4269"
    sp    <- sp::SpatialPoints(coord)
    spdf  <- sp::SpatialPointsDataFrame(sp, coord)
    sp::proj4string(spdf) <- proj #CRS(proj)

    #Insert attributes into the SpatialPointsDataFrame 
    spdf$name	 <- station.df$id
    #filter       <- name_and_date.df[ name_and_date.df$id %in% station.df$id, ] 
    spdf$mindate <- station.df$mindate
    spdf$maxdate <- station.df$maxdate
   
    OGRstring   <- base::paste("PG:dbname=", config$dbname, " user=", config$dbuser," password=", config$dbpwd, " host=", config$dbhost," port=", config$dbport, sep = "")
       
    coord_error <- rgdal::writeOGR(spdf, OGRstring, layer_options = "geometry_name=geom", overwrite_layer=TRUE, tableName, driver=driver, verbose='TRUE')  
   
    if(config$isgraphic){
      gWidgets::gmessage("Finished. Check Postgres table!\n")
    }else{cat("Finished. Check Postgres table\n")}
  
    station.df <- spdf 
   
  }#endIF/ELSE
  
  RPostgres::dbDisconnect(conn)

  return(tableName)
}#endFUNCTION
