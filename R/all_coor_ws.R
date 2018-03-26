#' all_coor_ws() retrieve weather station within a region ( State/County )  
#'
#' The function retrieves all NOAA weather station (ws) data available in a region ( State/County ) related
#' to the weather variable under investigation (type).  
#'
#' @keywords NOAA, weather stations, rnoaa
#' @param ghcnd  String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid  FIPS number from census tables (tiger files)
#' @param type   Variable name under investigation i.e. TMAX, TMIN, PRCP
#' @return The function returns a data frame. When it fails to retrieve information, it returns 1.
#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'PRCP'
#' all_coor_ws( ghcnd, geoid, type)
#' @export
all_coor_ws <- function( ghcnd, geoid, type) {

  file    <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
  config  <- yaml::yaml.load_file(file)
  FIPS    <- base::paste("FIPS:", geoid, sep = "")

  # Retrieve station info
  ws      <- rnoaa::ncdc_stations(datasetid=ghcnd, datatypeid=type, locationid=FIPS, limit=1000, token=config$token) 
  stations<- ws$data

  if ( length( which( stations$id == FALSE) ) > 0  ) {
      msg <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
      cat( msg )
    return('1')
  }

  switch(type,
         PRCP = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMAX = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMIN = stations <- subset(stations, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
  )

  stations$id <- gsub("GHCND:", "", stations$id)
 
  return(stations)

}#endFUNCTION
