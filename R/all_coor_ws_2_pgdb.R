#' all_coor_ws_2_pgdb retrieves NOAA data and creates a table in a Postgres database 
#'
#' The function retrieves the coordinates (coor) longitude and latitude; geometry of that location;
#' name of NOAA weather stations (ws), as well as minimum and maximum date of available information.
#' It stores all the data with in a table in a Posgresql (pg) database (db) with POSTGIS extension 
#' installed on it. The function necessitates arguments: ghcnd, FIPS, type, geoid. The sufix argument
#' is used for naming the table in the database. 
#' The dbhost, dbport, dbname, dbuser, dbpasswd, isgraphic, and noaa_token (key to NOAA service) are 
#' specified in a local file named pg_conf.yml that will be read in by yaml. See file format below. 
#' As it should be clear by now, this package assumes you have installed a Postgres database and 
#' the right permissions to create/read/write on it. 
 
#' @keywords NOAA, weather stations, rnoaa, Postgres
#' @param ghcnd  String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid  FIPS number from census tables
#' @param type   Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param sufix Substring to named the beginnig of Postgres table
#' @return It returns the name of the table that was created or the string "one" when fail to find weather station information.
#'
#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'PRCP'
#' sufix <- 'all_coor'
#' all_coor_ws_2_pgdb( ghcnd, geoid, type, sufix )
#' @note
#' pg_config.yml has the following structure
#' 
#' dbhost : serverName (default localhost)
#' 
#' dbport : portNumber (default 5432)
#' 
#' dbname : databaseName
#' 
#' dbuser : userName
#' 
#' dbpwd  : password
#' 
#' isgraphic: 0 (zero when gWidgets is not available) 
#' 
#' token  : NOAA keypass
#' 
#' The output table name has the following form: st_co_geoid_sufix_ws_type
#' "st" is the U.S. State name and  "co" is the U.S. County name. The sufix and type are specified by the user.
#' The function assumes that a cb_2013_us_state_20m and a cb_2013_us_county_20m exist in the postgres database.
#' @export 
all_coor_ws_2_pgdb <- function( ghcnd, geoid, type, sufix ){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  FIPS    <- base::paste("FIPS:", geoid, sep="")

  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("Downloading data and creating Postgresql table \t\t\t\t", expand=TRUE, container=gp)

  }

  ws <- rnoaa::ncdc_stations(datasetid=ghcnd, datatypeid=type, locationid=FIPS, limit=1000, token=config$token) 

  stations <- ws$data
  stations$id <- gsub("GHCND:", "", stations$id)
  if( length( which( stations$id == FALSE) ) > 0  ){
    if ( config$isgraphic ){
      gWidgets::svalue(txt) <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
    }else{
      msg <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
      cat( msg )
    }
    return('1')
  }

  if( config$isgraphic ){

    switch(type,
         PRCP = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMAX = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMIN = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
    )

  }

  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if( as.integer(geoid) < 100){  

    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q1)
    state <- RPostgres::dbFetch(res) 
    RPostgres::dbClearResult(res)
    tableName        <- base::paste(state,"_",geoid,"_ws_",sufix,"_",sep="")

  }else{

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    state  <- RPostgres::dbFetch(res) 
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_",sufix,"_",sep="")

  }

  varTable           <- tolower( tableName  )
  type               <- tolower(  type  )
  tableName          <- base::paste( varTable, type, sep="")
  tableName	     <-  gsub(" ", "_", tableName)
  
  if(RPostgres::dbExistsTable(conn, tableName)){

    if ( config$isgraphic ){

      gWidgets::svalue(txt) <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep="" )
      Sys.sleep(3)    
      gWidgets::gmessage("Check Postgresql table.")

    }

    print(base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep="" ))
    RPostgres::dbDisconnect(conn)
    ##RPostgres::dbUnloadDriver(drv)

    return(tableName)

  }else{

    if ( config$isgraphic ){

      msg <- base::paste("Creating ", tableName, " table of ", type , sep="")    
      gWidgets::svalue(txt) <- msg #"Creating table ...\t\t\t\t\t"

    }

    print( base::paste("Creating ", tableName, " table of ", type , sep="") )
    Sys.sleep(3)
    coord             <- base::as.data.frame( stations[,c("longitude","latitude")] )
    proj              <- "+init=epsg:4269"
    sp                <- sp::SpatialPoints(coord)
    spdf              <- sp::SpatialPointsDataFrame(sp, coord)
    sp::proj4string(spdf) <- proj #CRS(proj)

    #Insert attributes into the SpatialPointsDataFrame 
    spdf$mindate      <- stations$mindate
    spdf$maxdate      <- stations$maxdate
    spdf$name	      <- stations$id
    
    OGRstring         <- base::paste("PG:dbname=", config$dbname, " user=", config$dbuser," password=", config$dbpwd, " host=", config$dbhost," port=", config$dbport, sep = "")
    coord_error       <- rgdal::writeOGR(spdf, OGRstring, layer_options = "geometry_name=geom", tableName, driver=driver, overwrite_layer='TRUE', verbose='TRUE')

    print("Done! check Postgresql table.")
    
  }#endIF/ELSE

  RPostgres::dbDisconnect(conn)
  ##RPostgres::dbUnloadDriver(drv)      
  return(tableName)

}#endFUNCTION
