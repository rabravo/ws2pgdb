#' df_2_pgdb() stores weather station in database   
#'
#' This function takes a data frame (with weather stations' information)
#' and saves it as a table in a Postgres database. The function necessitates arguments: ghcnd, type, geoid,
#' and the data frame.
#'
#' @keywords NOAA, weather station, rgdal, RPostgreSQL, rnoaa
#' @param ghcnd    String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid    FIPS number from census tables
#' @param type     Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param coor_ws  Data frame with noaa weather station information
#' @param sufix   Help to name table at the database
#' @return Function returns a data frame. When fails store the information, it return 1.
#' @examples
#' ghcnd  <- 'GHCND'
#' geoid  <- '36061'
#' type   <- 'PRCP'
#' sufix <- 'example'
#' ncdc.df <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' df_2_pgdb( ghcnd, geoid, type, ncdc.df, sufix )

#' @note 
#' The output table name has the following form: st_co_geoid_ws_sufix_type 
#' "st" is the U.S. State, and  "co" is the U.S. County name. "geoid" is the fips identifier.
#' "sufix" a character string to identify table,and "type" is the weather variable (i.e. TMAX,TMIN,PRCP).
#' The function assumes that a cb_2013_us_state_20m and a cb_2013_us_county_20m exist in the postgres database.
#' @export
df_2_pgdb <- function(ghcnd, geoid, type, coor_ws, sufix){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )


  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  if( as.integer(geoid) < 100){
    
    q1  <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <-RPostgres::dbSendQuery(conn, q1)
    state <-  RPostgres::dbFetch(res) 
    RPostgres::dbClearResult(res)
    tableName        <- base::paste(state,"_",geoid,"_ws_",sufix,"_",sep="")

  }else{

    q2  <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <-RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res) 
    RPostgres::dbClearResult(res)
    q3  <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <-RPostgres::dbSendQuery(conn, q3)
    state <- RPostgres::dbFetch(res) 
    RPostgres::dbClearResult(res)
    tableName<- base::paste(state,"_",county,"_",geoid,"_ws_",sufix,"_",sep="")

  }

  varTable   <- tolower( tableName  )
  type       <- tolower( type  )
  tableName  <- base::paste( varTable, type, sep="")
  tableName  <- gsub(" ", "_",tableName)

  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("", expand=TRUE, container=gp)

  }

  if(RPostgres::dbExistsTable(conn, tableName)){

    if ( config$isgraphic ){

      txt <- gWidgets::glabel(base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep="" ), expand=TRUE, container=gp)
      Sys.sleep(3)    
      gWidgets::gmessage("Check Postgresql table.")

    }

    msg <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\n", sep="" )
    cat(msg)
    RPostgres::dbDisconnect(conn)
    return(tableName)

  }else{
    
    if ( config$isgraphic ){

      msg  <- base::paste("Creating ", tableName, " table of ", type , sep="")    
      gWidgets::svalue(txt) <- msg
      Sys.sleep(3)

    }
    #name_and_date.df  <- stations$data[,c("id","mindate","maxdate","longitude","latitude")]
    coord <- coor_ws[,c("longitude","latitude")]

    proj  <- "+init=epsg:4269"
    sp    <- sp::SpatialPoints(coord)
    spdf  <- sp::SpatialPointsDataFrame(sp, coord)
    sp::proj4string(spdf) <- proj #CRS(proj)

    #Insert attributes into the SpatialPointsDataFrame
    spdf$name	<- coor_ws$id
    spdf$mindate<- coor_ws$mindate
    spdf$maxdate<- coor_ws$maxdate

    OGRstring<- base::paste("PG:dbname=",config$dbname," user=",config$dbuser," password=",config$dbpwd, " host=",config$dbhost," port=",config$dbport,sep = "")

    #Need handling error
    coord_error<- rgdal::writeOGR(spdf,OGRstring,layer_options = "geometry_name=geom", tableName, driver=driver, overwrite_layer = 'TRUE', verbose = 'TRUE')

    if(config$isgraphic){ gWidgets::gmessage("Finished. Check Postgresql table!") }
    else{ print("Finished. Check Postgresql table!")  }

  }#endIF/ELSE

  RPostgres::dbDisconnect(conn)
  #RPostgres::dbUnloadDriver(drv)   
  return(tableName)

}