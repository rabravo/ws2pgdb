#' all_coor_ws() retrieve weather station within a region ( State/County )  
#'
#' The function retrieves all NOAA weather station (ws) data available in a region ( State/County ) related
#' to the weather variable under investigation (type).  
#'
#' @keywords NOAA, weather stations, rnoaa
#' @param ghcnd  String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid  FIPS number from census tables (tiger files)
#' @param type   Variable name under investigation i.e. TMAX, TMIN, PRCP
#' @return Data frame of NULL if error occurs
#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'PRCP'
#' all_coor_ws( ghcnd, geoid, type)
#' @export
all_coor_ws <- function( ghcnd, geoid, type) {

  tryCatch(
    {
      file    <- NULL
      path    <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
      file    <- yaml::yaml.load_file(path)
      FIPS    <- base::paste("FIPS:", geoid, sep = "")    
    },error = function (err_msg){  
        message(base::paste("msg: ", err_msg, sep=""))
        return(file)
    },warning = function (warn_msg){
        message(base::paste("msg: ", warn_msg, sep=""))
        return(file)
    }
  )

  # Check whether NOAA site returned any data
  tryCatch(
    {
      ws <- NULL
      stations <- NULL
      ws      <- rnoaa::ncdc_stations(datasetid=ghcnd, datatypeid=type, locationid=FIPS, limit=1000, token=config$token) 
      stations<- ws$data
    },error = function (err_msg){  
        message(base::paste("msg: ", err_msg, sep=""))
        return(stations)
    },warning = function (warn_msg){
        message(base::paste("msg: ", err_msg, sep=""))
        return(stations)
    }
  )  
  
  # Handle special case for FIPS 12087 where stations have identical data (?)
  if(geoid == '48113'){
      switch(type,
          PRCP = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
          TMAX = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
          TMIN = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
      )
  }
  prefix <- base::paste(ghcnd, ":", sep="")
  stations$id <- gsub(prefix, "", stations$id) 
  return(stations)

}#endFUNCTION
