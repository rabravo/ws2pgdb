#' all_coor_ws_2_pgdb retrieves NOAA data and creates a table in a PG database.
#'
#' The function retrieves the coordinates (coor) longitude and latitude; geometry of that location;
#' name of NOAA weather stations (ws), as well as minimum and maximum date of available information.
#' It stores all the data with in a table in a Posgresql (pg) database (db) with POSTGIS extension 
#' installed on it. The function necessitates arguments: ghcnd, FIPS, type, geoid. The suffix argument
#' is used for naming the table in the database. The dbhost, dbport, dbname, dbuser, dbpasswd,
#' and noaa_token (key to NOAA service) are specified in a local file named pg_conf.yml that will 
#' be read in by yaml. See file format below. As it should be clear by now, this package assumes 
#' you have installed a Postgres database and the right permissions to create/read/write on it. 
#' 
#' @keywords NOAA, weather stations, rnoaa, rgdal, PostgreSQL, rpostgis
#' 
#' @param ghcnd  String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid  FIPS number from census tables
#' @param type   Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param suffix Substring to named the beginnig of Postgres table
#' @return It returns the name of the table that was created or the string "one" when fail to find weather station information.
#'
#' @examples
#' all_coor_ws_2_pgdb(ghcnd = 'GHCND', geoid = '12087', type = 'PRCP', suffix = 'all_coor')
#' 
#' @note
#' pg_config.yml has the following structure:
#' dbhost : Host Name (default localhost)
#' dbport : Port Number (default 5432)
#' dbname : Database Name
#' dbuser : User Name
#' dbpwd  : Password
#' token  : NOAA keypass
#' 
#' The output table name has the following form: st_co_geoid_suffix_ws_type
#' "st" is the U.S. State name and  "co" is the U.S. County name. The suffix and type are specified by the user.
#' The function assumes that a cb_2013_us_state_20m and a cb_2013_us_county_20m exist in the postgres database.
#' 
#' @export 
all_coor_ws_2_pgdb <- function(ghcnd, geoid, type, suffix) {

  file <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
  config <- yaml::yaml.load_file(file)
  conn <- RPostgreSQL::dbConnect( drv      = "PostgreSQL", 
                                  host     = config$dbhost, 
                                  port     = config$dbport, 
                                  dbname   = config$dbname,
                                  user     = config$dbuser, 
                                  password = config$pwd
                                 )

  FIPS <- base::paste("FIPS:", geoid, sep = "")
  ws   <- rnoaa::ncdc_stations( datasetid  = ghcnd, 
                                datatypeid = type, 
                                locationid = FIPS, 
                                limit      = 1000,
                                token      = config$token
                              )

  stations    <- ws$data
  stations$id <- gsub("GHCND:", "", stations$id)
  if ( length( which( stations$id == FALSE) ) > 0  ) {
    msg <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep = "" )
    cat( msg )
    RPostgreSQL::dbDisconnect(conn)
    return('1')
  }

  switch(type,
       PRCP = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
       TMAX = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
       TMIN = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
  )
  
  if (as.integer(geoid) < 100) {
    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID = '", geoid, "'", sep = "")
    res   <- RPostgreSQL::dbSendQuery(conn, q1)
    state <- RPostgreSQL::fetch(res) 
    RPostgreSQL::dbClearResult(res)
    tableName <- base::paste(state, "_", geoid, "_ws_", suffix, "_", sep = "")

  } else {

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID = '", geoid, "'", sep = "")
    res    <- RPostgreSQL::dbSendQuery(conn, q2)
    county <- RPostgreSQL::fetch(res)
    RPostgreSQL::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID = '", substr(geoid, 1, 2), "'", sep = "")

    res    <- RPostgreSQL::dbSendQuery(conn, q3)
    state  <- RPostgreSQL::fetch(res) 
    RPostgreSQL::dbClearResult(res)
    tableName <- base::paste(state, "_", county, "_", geoid, "_ws_", suffix, "_", sep = "")
  }# endIF/ELSE

  varTable       <- tolower(tableName)
  type           <- tolower(type)
  tableName      <- base::paste(varTable, type, sep = "")
  tableName	     <-  gsub(" ", "_", tableName)
  
  if (RPostgreSQL::dbExistsTable(conn, tableName)) {

    print(base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep = "" ))
    RPostgreSQL::dbDisconnect(conn)
    return(tableName)

  } else {

    print( base::paste("Creating ", tableName, " table of ", type , sep = ""))
    pts    <- base::as.data.frame(stations[, c("longitude", "latitude")])
    coord  <- sp::SpatialPoints(pts)
    spdf   <- sp::SpatialPointsDataFrame(coord, pts)
    proj   <- "+init=epsg:4269"
    sp::proj4string(spdf) <- proj #CRS(proj)

    # Insert attributes into the SpatialPointsDataFrame 
    spdf$mindate  <- stations$mindate
    spdf$maxdate  <- stations$maxdate
    spdf$name     <- stations$id
    spdf$ogc_fid <- seq.int(nrow(stations))
    rpostgis::pgInsert(conn, name = c("public", tableName), data.obj = spdf, geom = "geom")
    print("Done! check Postgresql table.")
  }# endIF/ELSE
  RPostgreSQL::dbDisconnect(conn)
  return(tableName)
}# endFUNCTION
