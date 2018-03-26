#' df_2_pgdb() stores weather station in database   
#'
#' This function takes a data frame (with weather stations' information)
#' and saves it as a table in a Postgres database. The function necessitates arguments: ghcnd, type, geoid,
#' and the data frame.
#'
#' @keywords NOAA, weather station, RPostgreSQL, rnoaa
#' @param ghcnd    String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid    FIPS number from census tables
#' @param type     Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param coor_ws  Data frame with noaa weather station information
#' @param suffix   Help to name table at the database
#' @return Function returns a data frame. When fails store the information, it return 1.
#' 
#' @examples
#' ncdc.df <- as.data.frame(all_coor_ws(ghcnd = 'GHCND', geoid = '36061', type = 'PRCP'))
#' df_2_pgdb(ghcnd = 'GHCND', geoid = '36061', type = 'PRCP', ncdc.df, suffix = 'example')
#' 
#' @note 
#' The output table name has the following form: st_co_geoid_ws_suffix_type 
#' "st" is the U.S. State, and  "co" is the U.S. County name. "geoid" is the fips identifier.
#' "suffix" a character string to identify table,and "type" is the weather variable (i.e. TMAX,TMIN,PRCP).
#' The function assumes that a cb_2013_us_state_20m and a cb_2013_us_county_20m exist in the postgres database.
#' 
#' @export
df_2_pgdb <- function(ghcnd, geoid, type, coor_ws, suffix){

  file   <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
  config <- yaml::yaml.load_file( file )

  conn <- RPostgreSQL::dbConnect( drv      = "PostgreSQL",
                                  host     = config$dbhost, 
                                  port     = config$dbport, 
                                  dbname   = config$dbname,
                                  user     = config$dbuser, 
                                  password = config$pwd
                                )

  if (as.integer(geoid) < 100) {
    
    q1       <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid, "'", sep = "")
    res      <- RPostgreSQL::dbSendQuery(conn, q1)
    state    <- RPostgreSQL::fetch(res) 
    RPostgreSQL::dbClearResult(res)
    tableName<- base::paste(state, "_", geoid, "_ws_", suffix, "_", sep = "")

  } else {

    q2 	     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID = '", geoid, "'", sep = "")
    res      <- RPostgreSQL::dbSendQuery(conn, q2)
    county   <- RPostgreSQL::fetch(res) 
    RPostgreSQL::dbClearResult(res)
    q3       <- base::paste("select NAME from cb_2013_us_state_20m where GEOID = '", substr(geoid, 1, 2), "'", sep = "")
    res      <- RPostgreSQL::dbSendQuery(conn, q3)
    state    <- RPostgreSQL::fetch(res) 
    RPostgreSQL::dbClearResult(res)
    tableName<- base::paste(state, "_", county, "_", geoid, "_ws_", suffix, "_", sep = "")

  }# endIF/ELSE

  varTable   <- tolower(tableName)
  type       <- tolower(type)
  tableName  <- base::paste(varTable, type, sep = "")
  tableName  <- gsub(" ", "_", tableName)

  if (RPostgreSQL::dbExistsTable(conn, tableName)) {

    msg      <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\n", sep = "" )
    cat(msg)
    RPostgreSQL::dbDisconnect(conn)
    return(tableName)

  } else {
    
    coord <- coor_ws[, c("longitude", "latitude")]
    sp    <- sp::SpatialPoints(coord)
    spdf  <- sp::SpatialPointsDataFrame(sp, coord)
    proj  <- "+init=epsg:4269"
    sp::proj4string(spdf) <- proj #CRS(proj)

    # Insert attributes into the SpatialPointsDataFrame
    spdf$name	<- coor_ws$id
    spdf$mindate<- coor_ws$mindate
    spdf$maxdate<- coor_ws$maxdate
    spdf$ogc_fid <- seq.int(nrow(coor_ws))
    rpostgis::pgInsert(conn, name = c("public", tableName), data.obj = spdf, geom = "geom")
    print("Finished. Check Postgresql table!")
  }# endIF/ELSE
  RPostgreSQL::dbDisconnect(conn)
  return(tableName)
}
