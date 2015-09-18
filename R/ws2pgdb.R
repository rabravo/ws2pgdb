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

  stations <- rnoaa::ncdc_stations(datasetid=ghcnd, datatypeid=type, locationid=FIPS, limit=1000, token=config$token) 

  if( length( which( stations$data$id == FALSE) ) > 0  ){
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
         PRCP = stations$data <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMAX = stations$data <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMIN = stations$data <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
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
    coord             <- as.data.frame( stations$data[,c("longitude","latitude")] )
    proj              <- "+init=epsg:4269"
    sp                <- sp::SpatialPoints(coord)
    spdf              <- sp::SpatialPointsDataFrame(sp, coord)
    sp::proj4string(spdf) <- proj #CRS(proj)

    #Insert attributes into the SpatialPointsDataFrame 
    spdf$name	      <- stations$data$id
    spdf$mindate      <- stations$data$mindate
    spdf$maxdate      <- stations$data$maxdate

    
    OGRstring         <- base::paste("PG:dbname=", config$dbname, " user=", config$dbuser," password=", config$dbpwd, " host=", config$dbhost," port=", config$dbport, sep = "")
    coord_error       <- rgdal::writeOGR(spdf, OGRstring, layer_options = "geometry_name=geom", tableName, driver=driver, overwrite_layer='TRUE', verbose='TRUE')

    print("Done! check Postgresql table.")
    
  }#endIF/ELSE

  RPostgres::dbDisconnect(conn)
  ##RPostgres::dbUnloadDriver(drv)      
  return(tableName)

}#endFUNCTION






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
all_coor_ws <- function( ghcnd, geoid, type){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  FIPS<- base::paste("FIPS:", geoid, sep="")

  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("Downloading data \t\t\t\t", expand=TRUE, container=gp)

  }
  

  #Extract stations    
  stations <- rnoaa::ncdc_stations(datasetid=ghcnd, datatypeid=type, locationid=FIPS, limit=1000, token=config$token) 

  if( length( which( stations$data$id == FALSE) ) > 0  ){
    if ( config$isgraphic ){
      gWidgets::svalue(txt) <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
    }else{
      msg <- base::paste("Not Data Available for variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
      cat( msg )
    }
    return('1')
  }


  switch(type,
         PRCP = stations <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMAX = stations <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928"),
         TMIN = stations <- subset(stations$data, id != "GHCND:USW00013907" & id != "GHCND:USW00093928")
  )
  
  return(stations)

}#endFUNCTION





#' df_2_pgdb stores weather station in database   
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





#' canopiVoronoi() creates a table with the polygones of a Voronoi tessellation that intersect a State/County.
#'
#' The function takes the name of a existing table made of at least to columns: a column with geometric points
#' and a column with unique identifiers for each element row in the table. The function produces a table in a 
#' Postgres database with geometries that form a Voronoi tessellation ( polygones ). The arguments needed for 
#' the function are two: a table's name and geoid number.
#'
#' @keywords Voronoi, tessellation, weather station, plr, pl/r, RPostgreSQL, Postgres
#' @param tableName Table with geometric points used to generate Voronoi tessellation
#' @param geoid    The geoid is needed to identify the boundaries of intersection, 
#' where the generating points reside, that is in a State or a County.
#' @return Function returns the name of the table containing the polygones forming the Voronoi
#' or the value 1 when fail to find weather station information.
#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'PRCP'
#' sufix <- 'example'
#' tableName <- all_coor_ws_2_pgdb( ghcnd, geoid, type, sufix )
#' canopiVoronoi( tableName, geoid )
#' @note 
#' The output table name has the following form: st_co_geoid_ws_sufix_type_v_poly. 
#' "st" is the U.S. State name and  "co" is the U.S. County name. The "sufix" and "type" are 
#' specified by the user in the all_coor_ws_2_pgdb function. The following function assumes 
#' you have postgresql-9.3, postgis-2.1, and postgresql-9.3-plr extension installed. Enable 
#' your database with both postgis and plr extensions with the following command in UBUNTU:
#' > sudo -u dbuser psql -c "CREATE EXTENSION postgis; CREATE EXTENSION plr; 
#' where dbuser is the owner of the database. Moreover, in https://gist.github.com/djq/4714788 
#' you will find the query code to be loaded into the database. The r_voronoi(text,text,text) 
#' function is needed to create the Voronoi tessellation. canopiVoronoi() function exploits 
#' this code for my study, but a small modification was required. The buffer_distance has been 
#' increased from 10% to 50% so that the tessellation guarantees to cover the whole area under study. 
#' When using the functions from this package to produce the table with the geometric points, as 
#' shown in the example, you should not have problems reusing canopiVoronoi(). 
#' @export
canopiVoronoi <- function( tableName, geoid ){


  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  driver <- "PostgreSQL"

  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("Downloading data and creating Postgresql table \t\t\t\t", expand=TRUE, container=gp)

  }else{

    print("Downloading data and creating Postgres table ...\t\t\t\t ")

  }

  if( !is.null( tableName ) ){   
 
     drv  <- RPostgres::Postgres()
     conn <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
     voronoiTableName  <- as.character( base::paste( tableName, "_v_poly",sep="") )

    if( RPostgres::dbExistsTable( conn, voronoiTableName) ){

      if ( config$isgraphic ){

        gWidgets::svalue( txt ) <- base::paste("Done -Table ", voronoiTableName, " exists.\t\t\t\t\t", sep="" )
        Sys.sleep( 3 )    
        gWidgets::gmessage( "Check Postgresql table." )

      }else{

        txt <- base::paste("Done - Table ", voronoiTableName, " exists.\t\t\t\t\t", sep="" )
        cat(txt)

      }

      RPostgres::dbDisconnect(conn)
      #RPostgres::dbUnloadDriver(drv)
      return( voronoiTableName )

    }

    #Intersection between the boundary of the county and the voronoi tesselation     
    if( as.integer(geoid) < 100){ tigerDB <- "cb_2013_us_state_20m" }else{ tigerDB <- "cb_2013_us_county_20m"} 

    geoid        <- as.character( geoid )
    voronoiTable <- base::paste(" r_voronoi('", tableName ,"', 'geom' , 'ogc_fid' )", sep="")
    intersection <- base::paste(
	"with \
	 county  AS(\
		   SELECT ST_SetSRID(geom, 4326) as geom FROM ", tigerDB , " WHERE geoid= CAST( ", geoid ," As text) ),\
         voronoi AS(\
		   SELECT id, ST_SetSRID( polygon , 4326) as polygon FROM ", voronoiTable , "),\
	 output AS(\
		   SELECT id, ST_Intersection( v.polygon, t.geom) as v_polygon FROM voronoi as v, county as t )\
	 SELECT * INTO ", voronoiTableName , " FROM output;" , sep="")

    #Invoke the system to execute the query
    #psql_query         <- base::paste("psql -U ", dbuser, " -d us_gisdb -c \"", intersection, "\"", sep="")
    #msg <- system(psql_query) 

    query   <- intersection
    res <- RPostgres::dbSendQuery(conn, intersection)
    msg <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    RPostgres::dbDisconnect(conn)
    #RPostgres::dbUnloadDriver(drv)           
    
    return(voronoiTableName)

  }else{
   
       return('1')
  }

}#endFUNCTION


#' createVoronoi creates a Voronoi tessellation
#'
#' This function creates a table with polygone geometries of a Voronoi tessellation in a Postgres database.
#'
#' @param tableName The name of existing table within the Postgres database.
#' @param the_geom The column name with the geometries of the generating points.
#' @param ogc_fid  The column name with the unique identifiers for each row element (point).
#' @param scale The percentage of buffer distance (how far the expand outward) 
#' @return The function returns the name of the voronoi table that has been created. 
#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '40'
#' type  <- 'TMAX'
#' sufix <- 'create_voronoi_example'
#' tableName <- all_coor_ws_2_pgdb( ghcnd, geoid, type, sufix )
#' createVoronoi(tableName, "geom", "ogc_fid", 0.25)
#'
#' @note In https://gist.github.com/djq/4714788 you will find the query code to be loaded into the database. 
#' The r_voronoi(text,text,text) function is needed to create the Voronoi tessellation. 
#' createVoronoi() function exploits this code, but a small modification was required. 
#' The buffer_distance has been made flexible in contrast to the original code that sets the buffer distance to 10%. 
#' The r_voronoi(text,text,text) has been replaced by the r_voronoi_scale(text,text,text,float), 
#' and TYPE was renamed as the r_voronoi_scale_type. In the original code, search for the section with the comment 
#' "calculate an approprate buffer distance (~10%):" and replace (0.10) for (arg4). That should suffice 
#' to make createVoronoi() to work as explained in the example.  
#' @export
createVoronoi <- function( tableName, the_geom, ogc_fid, scale){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  driver <- "PostgreSQL"

  if ( config$isgraphic ){

    w   <- gWidgets::gwindow("Message: ", width=500)
    gp  <- gWidgets::ggroup(container=w, expand=TRUE)
    txt <- gWidgets::glabel("Downloading data and creating Postgresql table \t\t\t\t", expand=TRUE, container=gp)

  }else{

    print("Downloading data and creating Postgres table ...\t\t\t\t ")

  }

  if( !is.null( tableName ) ){   
    
    drv  <- RPostgres::Postgres()
    conn <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
    voronoiTableName  <- as.character( base::paste( tableName, "_v_poly",sep="") )

    if( RPostgres::dbExistsTable( conn, voronoiTableName) ){

      if ( config$isgraphic ){
   
        gWidgets::svalue( txt ) <- base::paste("Done -Table ", voronoiTableName, " exists.\t\t\t\t\t", sep="" )
        Sys.sleep( 3 )    
        gWidgets::gmessage( "Check Postgresql table." )
      
      }else{

        txt <- base::paste("Done - Table ", voronoiTableName, " exists.\t\t\t\t\t", sep="" )
        cat(txt)
      }

      RPostgres::dbDisconnect( conn )
      #RPostgres::dbUnloadDriver(drv)
      return( voronoiTableName )

    }

    query <-base::paste(" select * into ", voronoiTableName ," from r_voronoi_scale('", tableName , "','" , the_geom , "', '", ogc_fid , "',", scale, " );", sep="")

    #Alternative: invoke system() to execute the query
    #psql_query <- base::paste("psql -U ", dbuser, " -d ", dbname, " -c \"", query, "\"", sep="")
    #msg <- system(psql_query) 
    res <- RPostgres::dbSendQuery( conn, query)
    msg <- data.frame(RPostgres::dbFetch(res))   
    RPostgres::dbClearResult(res)
    RPostgres::dbDisconnect(conn)
    #RPostgres::dbUnloadDriver(drv)           
    
    return(voronoiTableName)

  }else{

    return('1')
  }

}#endFUNCTION








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
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' ws_metadata_2_pgdb( geoid, type, stations ) 
#' @note Remember that all_coor_ws() returns a set of stations.
#' @export
ws_metadata_2_pgdb <- function( geoid, type, stations){

  base::options(guiToolkit="tcltk") 
  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  if( as.integer(geoid) < 100){  
  
    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q1)
    state <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_", geoid,"_ws_metadata_", sep="")

  }else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q2)
    county<- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgres::dbSendQuery(conn,q3)
    state <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    tableName<- base::paste(state, "_",county,"_",geoid,"_ws_metadata_", sep="")

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


    msg <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")

    if ( config$isgraphic ){
      gWidgets::svalue(txt) <- msg
      #svalue(txt2) <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\t", sep="" )
      Sys.sleep(3)    
      gWidgets::gmessage("Check Postgresql table.")
    } else { cat(msg) }
    
      RPostgres::dbDisconnect(conn)
      return(tableName)    

  } else {

    name_and_date.df<- stations[,c("id","mindate","maxdate","longitude","latitude")]

    #Sorted Indexes
    order.maxdate   <- order(name_and_date.df$maxdate, decreasing = TRUE)   	

    #Get the intervals sort in decreasing order      
    stationSortByMaxDate <- name_and_date.df[order.maxdate,]                 	

    #Filter station with datacoverage > 90%
    #subIntervalDataCover      <- subset( stationSortByMaxDate, datacoverage >= 0.90 )

    #Filter station with maxdate year in 2015
    subIntervalYear <- subset( stationSortByMaxDate, lubridate::year(  maxdate ) == lubridate::year( lubridate::today() ) )

    #Update date to beginning of current year  
    subIntervalYear$maxdate <- base::as.Date( lubridate::floor_date( lubridate::today() , "year") )  	

    #Sorted Indexes
    order.mindate   <- order( subIntervalYear$mindate, decreasing = TRUE)    

    #Get the intervals sort in decreasing order
    stationSortByMinDate <- subIntervalYear[order.mindate,]                  	

  
    #Guarantees an intersection of data of all weather stations from current year back to
    #a threshold of years. (i.e. current year 2015, and threshold equal 10 or 2005)
    threshold   <- 10

    #When the number of available stations is LESS than numStations, code chooses all stations; 
    #otherwise, it uses the REPEAT iteration to gather at least five stations. 
    numStations <- 5
  
   
    if(  !( isTRUE( nrow( stationSortByMinDate ) < numStations ) )  ){

      #Theshold guarantees latest information    
      thrs <- lubridate::year( base::as.Date( lubridate::floor_date(  lubridate::today() , "year") ) ) - threshold
    
      subIntervalMinSizeOfAYear <- subset(stationSortByMinDate,  lubridate::year( mindate ) <= thrs ) 
    
      #Heursitics: when number of weather stations is small, this section will intend to increase the 
      #number of weather stations while reducing the threshold value.
    
      repeat{
    
        #The number of minimum weather stations     
        if( !( isTRUE( nrow(subIntervalMinSizeOfAYear) >= numStations - 1 ) ) ){        
          thrs <- thrs - 1
          subIntervalMinSizeOfAYear <- subset(stationSortByMinDate,  lubridate::year( mindate  ) <= thrs ) 
        }else{break}#endIF/ELSE
      
      }#endREPEAT
    
    }else{ 
         order.min <- order( stationSortByMinDate$mindate, decreasing = TRUE)
         subIntervalMinSizeOfAYear <- stationSortByMinDate[order.min,]
	#subIntervalMinSizeOfAYear <- stationSortByMinDate

    }

    #Extract the sorted element with the earliest start time  
    startDate <- subIntervalMinSizeOfAYear$mindate[1]                     
    
    #Update dates to startDate value to all elements in the array
    subIntervalMinSizeOfAYear$mindate <- base::as.Date(startDate)

    #Simplistic naming return this data structure
    station.df <- subIntervalMinSizeOfAYear                                  
    
    msg <- base::paste("Creating ", tableName, " table of ", type , sep="")    
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
      gWidgets::gmessage("Finished. Check Postgres table!")
    }else{cat("Finished. Check Postgres table")}
  
    station.df <- spdf 
   
  }#endIF/ELSE
  
  RPostgres::dbDisconnect(conn)
  #RPostgres::dbUnloadDriver(drv)      
  return(tableName)
}#endFUNCTION







#' ws_data_na_2_pgdb() retrieves the subset of weather data available in the intersecting weather stations
#'
#' Retrieves the weather data from the intersecting stations. It is known that NOAA listed
#' available data for certain years, months, or days; however, some of this information is missing. 
#' This function replaces the missing data with a NA value of the dataset. These value is read by 
#' the Postgres database as an empty cell. You can confirm this using pgAdmin3 or other database manager.
#' The purpose of this function is to store the raw data, as it is, so that, the user can decide
#' what to do with missing points ( i.e. extrapolate). The example shows the use of ws_metadata_span_2_pgdb()
#' , but it can be very well be replaced by ws_metadata_2_pgdb().  
#'
#' @keywords NOAA, weather station, rgdal, RPostgreSQL, rnoaa
#' @param ghcnd    String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid    FIPS number from census tables
#' @param type     Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param ws_metadata  The table name of the subset of weather stations with intersecting data
#' @return Returns the name of the table that has been created. When fail, it returns 1.

#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' ws_metadata  <-  ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' ws_data_na_2_pgdb(ghcnd, geoid, type, ws_metadata)
#'
#' @export
ws_data_na_2_pgdb <- function( ghcnd, geoid, type, ws_metadata){

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
    tableName <- base::paste(state, "_",geoid,"_ws_data_na_", sep="")

  }else{

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    state  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_data_na_", sep="")

  }

  type      <- tolower(  type  )
  tableName <- gsub(" ", "_", tableName)
  tableName <- tolower( tableName ) 
  tableName <- base::paste( tableName, type, sep="")      
  #print( tableName )

  if(config$isgraphic){

    w    <- gWidgets::gwindow("Message: ", width=500)
    gp   <- gWidgets::ggroup(container=w, expand=TRUE)
    txt  <- gWidgets::glabel("Creating Postgres weather information table \t\t\t\t", expand=TRUE, container=gp)

  }else{ cat("Creating Postgres weather information table \t\t\t\n")}
  
  if(RPostgres::dbExistsTable(conn, tableName)){

    msg <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")

    if(config$isgraphic){

      gWidgets::svalue(txt) <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")
      Sys.sleep(5)    
      gWidgets::gmessage("Check Postgres table!\n")

    } else { 

      cat(msg)
      cat("Check Postgres table!\n")

    }

    RPostgres::dbDisconnect(conn)
    return(tableName)
    
  }else{
    
    q1 <- base::paste("select name as id, mindate as mindate, maxdate as maxdate, longitude as longitude, latitude as latitude from ", ws_metadata, sep="")
    res <- RPostgres::dbSendQuery(conn, q1)
    station <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)

    minDate <- base::as.Date( lubridate::ymd( station$mindate[1] ) )
    maxDate <- base::as.Date( lubridate::ymd( station$maxdate[1] ) )
	  
    #One is end in a new year.  
    ndays <- ( lubridate::int_length( lubridate::new_interval( base::as.Date( minDate ) , base::as.Date( maxDate )))/(3600*24))+1 
    dates <- as.character( seq.Date( minDate , by ="days", length.out= ndays) )

    #The NOAA API only allows queries from users to retrieve data sets by year; 
    #thus, this code section limits each query to a different year for each iteration.  
    #WARNING: There is 500 limit request per day with the current token 
	
    for(i in 1:nrow( station ) ){  
	
      estacion	 <- station$id[i]
      #print(estacion)
      #startDate
      sDate	 <- minDate
      #finishDate                 
      fDate	 <- minDate
	    
      #Used for testing Comment this out after testing ( maybe deprecated)        
      #year( sDate )	<- year( maxDate ) - 5   
      #year( fDate )	<- year( maxDate ) - 5
	    
      lubridate::year( fDate ) <- lubridate::year( fDate ) + 1    #Interval (sDate, fDate) is one year.

      #Temporal strings attached to the dates for consistency with NOAA 	    
      ssDate <- base::paste(sDate, "T00:00:00", sep="")
      ffDate <- base::paste(fDate, "T00:00:00", sep="")
	  
      weatherVar <- rnoaa::ncdc(datasetid=ghcnd, stationid=estacion, datatypeid=type, startdate=ssDate, enddate=ffDate , limit=366, token=config$token)
      #Verify that available information exist
      if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
        if ( config$isgraphic ){
          gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
        }else{
          msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".", sep="" )
          cat( msg )
        }

        colnames( valores ) <- station$id[1:(i-1)]
        RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
        RPostgres::dbDisconnect( conn )
        return(tableName) 

      } 

      weatherYear<- weatherVar$data

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
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for statino ", estacion, " for variable type:  ", type, ".\t\n", sep="" )
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
	 
	      }else{ 
	      
	        lubridate::month( fDate ) <- lubridate::month( maxDate ) 
	        lubridate::day(   fDate ) <- lubridate::day(   maxDate ) 
	        ssDate         <- base::paste( sDate, "T00:00:00", sep="")
	        ffDate         <- base::paste( fDate, "T00:00:00", sep="")                      
	      
	        weatherVar<-rnoaa::ncdc(datasetid = ghcnd, stationid = estacion, datatypeid=type, startdate = ssDate, enddate= ffDate , limit=366, token=config$token)

	        #Verify that available information exist
      		if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
                  if ( config$isgraphic ){
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\n", sep="" )
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
	      
	    }

	    # TMAX = Maximum temperature (tenths of degrees C)	    
	    #promedio <- sprintf( "%.4f", mean( weatherYear$value/10 ) )     
	    
	    # Return year mean value  
	    #print( promedio )                                               

	    #NOAA specification of available data within a range often is not complete. 
	    #Individual dates are missing even within an specified range.
	    #SOLUTIONS:  
	    #1) Average from the available number of samples.  
	    #2) Interpolation when possible

	
	    if ( i == 1)
	    {
	    
	      intervalAsDate     <-  base::as.Date( weatherYear$date ) 
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
	        
	        #Main change from conn_rnooa_postgresql_writeOGR_locs_and_weatherData_v2 (v2)
		# is the following line. We replace average for Interpolated values.
	        #In v2 the vecPromedio variable hold the average value of the available data.
		#Currently as can be observed, it is replaced by NA that is replaced by an empty
		#cell when exported to a database with column holding a variable of type double
	        vecPromedio     <- rep( NA, sizeSubV1  )	
	        
		#Update allDatesHash values found in 'dataKeys' with NA 
	        hash::values( allDatesHash, keys = dateKeys) <- vecPromedio
	                 
	        valores <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )

	      }else{ valores  <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
	    }
	    else
	    {
	    
	      #cat("In ELSE section\n")
	      #print(estacion)
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
	        
	        #Main change from v2 is this line. We replace average for Interpolated values.
	        #In v2 the vecPromedio the current NA values were replaced instead by an average value.
	        vecPromedio      <- rep( NA, sizeSubV1  )
	        hash::values( allDatesHash, keys=dateKeys) <- vecPromedio
	        subValores       <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )            
	        valores          <- data.frame( cbind( valores, subValores ) )	        

	      }else{ valores     <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
	    }
	        	    
            rm( stationDataHash )	
        
	  }#endFOR   (stick time series of different stations together)	
	  
	  colnames( valores ) <- station$id
#      print( tail( valores) )
      
      if (config$isgraphic){	
        w2   <- gWidgets::gwindow("Message: ", width=500)
        gp2  <- gWidgets::ggroup(container=w2, expand=TRUE)
        txt2 <- gWidgets::glabel("", expand=TRUE, container=gp2)	
        gWidgets::svalue( txt2 ) <- base::paste("Creating table ", tableName, ".\t\t\t\t\t", sep="")
      }else{
        msg <- base::paste("Creating table ", tableName, ".\t\t\t\t\t", sep="")
        cat(msg)
      }
      RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
      Sys.sleep(4)
      if(config$isgraphic){
        gWidgets::gmessage("Check Postgres table!")    
      }else{cat("Check Postgres table!")}
	  
  }#endIF/ELSE
   
  RPostgres::dbDisconnect( conn )
  return(tableName) 

}#end FUNCTION_dateIntervals





#' ws_data_avg_2_pgdb() retrieves the subset of ws with intersectin data
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
#' @param ws_metadata  The table name of the subset of weather stations with intersecting data
#' @return Returns the name of the table that has been created. When fail, it returns 1.

#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <-'2'
#' ws_metadata  <- ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
#' ws_data_avg_2_pgdb( ghcnd, geoid, type, ws_metadata)
#'
#' @export
ws_data_avg_2_pgdb <- function( ghcnd, geoid, type, ws_metadata){

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
    tableName <- base::paste(state, "_",geoid,"_ws_data_avg_", sep="")

  }else{

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    state  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_data_avg_", sep="")
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
	  ndays <- ( lubridate::int_length( lubridate::new_interval( base::as.Date( minDate ) , base::as.Date( maxDate )))/(3600*24))+1 
	  dates <- as.character( seq.Date( minDate , by ="days", length.out= ndays) )
	
	  #The NOAA API only allows queries from users to retrieve data sets by year; 
	  #thus, this code section limits each query to a different year for each iteration.  
	  #WARNING: There is 500 limit request per day with the current token 
	
	  for(i in 1:nrow( station ) ){  
	
	    estacion	 <- station$id[i]
	    #startDate
	    sDate	 <- minDate
	    #finishDate                 
	    fDate	 <- minDate
	    
	    lubridate::year( fDate ) <- lubridate::year( fDate ) + 1    #Interval (sDate, fDate) is one year.

	    #Temporal strings attached to the dates for consistency with NOAA 	    
	    ssDate <- base::paste(sDate, "T00:00:00", sep="")
	    ffDate <- base::paste(fDate, "T00:00:00", sep="")
	  
	    weatherVar <- rnoaa::ncdc(datasetid=ghcnd, stationid=estacion, datatypeid=type, startdate=ssDate, enddate=ffDate , limit=366, token=config$token)
	    #Verify that available information exist
            if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
              if ( config$isgraphic ){
                gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion," with variable type:  ", type, " exists.\t\t\t\t", sep="" )
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
	      
	    }

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

	      }else{ valores  <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
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
	        valores          <- data.frame( cbind( valores, subValores ) )	        

	      }else{ valores     <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
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

}#end FUNCTION_dateIntervals






#' ws_metadata_span_2_pgdb Stores NOAA information into local or remote database server
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
	  ndays <- ( lubridate::int_length( lubridate::new_interval( base::as.Date( minDate ) , base::as.Date( maxDate )))/(3600*24))+1 
	  dates <- as.character( seq.Date( minDate , by ="days", length.out= ndays) )
	
	  #The NOAA API only allows queries from users to retrieve data sets by year; 
	  #thus, this code section limits each query to a different year for each iteration.  
	  #WARNING: There is 500 limit request per day with the current token 
	
	  for(i in 1:nrow( station ) ){  
	
	    estacion	 <- station$id[i]
	    #startDate
	    sDate	 <- minDate
	    #finishDate                 
	    fDate	 <- minDate
	    
	    lubridate::year( fDate ) <- lubridate::year( fDate ) + 1    #Interval (sDate, fDate) is one year.

	    #Temporal strings attached to the dates for consistency with NOAA 	    
	    ssDate <- base::paste(sDate, "T00:00:00", sep="")
	    ffDate <- base::paste(fDate, "T00:00:00", sep="")
	  
	    weatherVar <- rnoaa::ncdc(datasetid=ghcnd, stationid=estacion, datatypeid=type, startdate=ssDate, enddate=ffDate , limit=366, token=config$token)
	    #Verify that available information exist
            if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
              if ( config$isgraphic ){
                gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion," with variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
              }else{
                msg <- base::paste("Not Data Available for station ", estacion ," with variable type:  ", type, " exists.\t\t\t\t\t", sep="" )
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
	      
	    }

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

	      }else{ valores  <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
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
	        valores          <- data.frame( cbind( valores, subValores ) )	        

	      }else{ valores     <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
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










#' ws_data_na_span_2_pgdb() retrieves the subset of ws with data available in the intersecting ws
#'
#' Retrieves the weather data from the intersecting stations. It is known that NOAA listed
#' available data for certain years, months, or days; however, some of this information is missing. 
#' This function replaces the missing data with a NA value of the dataset. These value is read by 
#' the Postgres database as an empty cell. You can confirm this using pgAdmin3 or other database manager.
#' The purpose of this function is to store the raw data, as it is, so that, the user can decide
#' what to do with missing points ( i.e. extrapolate). The example shows the use of ws_metadata_span_2_pgdb()
#' , but it can be very well be replaced by ws_metadata_2_pgdb().  
#'
#' @keywords NOAA, weather station, rgdal, RPostgreSQL, rnoaa
#' @param ghcnd    String that refers to the Global Historical Climate Network (daily) dataset
#' @param geoid    FIPS number from census tables
#' @param type     Variable under investigation i.e. TMAX, TMIN, PRCP
#' @param span     Look-back time to search and retrieve weather data information
#' @param ws_metadata  The table name of the subset of weather stations with intersecting data
#' @return Returns the name of the table that has been created. When fail, it returns 1.

#' @examples
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' ws_metadata  <-  ws_metadata_span_2_pgdb( geoid, type, stations, span)
#' ws_data_na_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
#'
#' @export
ws_data_na_span_2_pgdb <- function( ghcnd, geoid, type, span, ws_metadata){

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
    tableName <- base::paste(state, "_",geoid,"_ws_data_na_span_",span,"_", sep="")

  }else{

    q2     <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3     <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    state  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_data_na_span_",span,"_",sep="")

  }

  type      <- tolower(  type  )
  tableName <- gsub(" ", "_", tableName)
  tableName <- tolower( tableName ) 
  tableName <- base::paste( tableName, type, sep="")      
  #print( tableName )

  if(config$isgraphic){

    w    <- gWidgets::gwindow("Message: ", width=500)
    gp   <- gWidgets::ggroup(container=w, expand=TRUE)
    txt  <- gWidgets::glabel("Creating Postgres weather information table \t\t\t\t", expand=TRUE, container=gp)

  }else{ cat("Creating Postgres weather information table \t\t\t\t")}
  
  if(RPostgres::dbExistsTable(conn, tableName)){

    msg <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")

    if(config$isgraphic){

      gWidgets::svalue(txt) <- base::paste("Done - Table ", tableName, " exists.\t\t\t\t\n", sep="")
      Sys.sleep(5)    
      gWidgets::gmessage("Check Postgres table!\n")

    } else { 

      cat(msg)
      cat("Check Postgres table!\n")

    }

    RPostgres::dbDisconnect(conn)
    return(tableName)
    
  }else{
    
    q1 <- base::paste("select name as id, mindate as mindate, maxdate as maxdate, longitude as longitude, latitude as latitude from ", ws_metadata, sep="")
    res <- RPostgres::dbSendQuery(conn, q1)
    station <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    #print(station)
    #return(station)
    minDate <- base::as.Date( lubridate::ymd( station$mindate[1] ) )
    maxDate <- base::as.Date( lubridate::ymd( station$maxdate[1] ) )
	  
    #One is end in a new year.  
    ndays <- ( lubridate::int_length( lubridate::new_interval( base::as.Date( minDate ) , base::as.Date( maxDate )))/(3600*24))+1 
    dates <- as.character( seq.Date( minDate , by ="days", length.out= ndays) )

    #The NOAA API only allows queries from users to retrieve data sets by year; 
    #thus, this code section limits each query to a different year for each iteration.  
    #WARNING: There is 500 limit request per day with the current token 
	
    for(i in 1:nrow( station ) ){  
	
      estacion	 <- station$id[i]
      #print(estacion)
      #startDate
      sDate	 <- minDate
      #finishDate                 
      fDate	 <- minDate
	    
      #Used for testing Comment this out after testing ( maybe deprecated)        
      #year( sDate )	<- year( maxDate ) - 5   
      #year( fDate )	<- year( maxDate ) - 5
	    
      lubridate::year( fDate ) <- lubridate::year( fDate ) + 1    #Interval (sDate, fDate) is one year.

      #Temporal strings attached to the dates for consistency with NOAA 	    
      ssDate <- base::paste(sDate, "T00:00:00", sep="")
      ffDate <- base::paste(fDate, "T00:00:00", sep="")
	  
      weatherVar <- rnoaa::ncdc(datasetid=ghcnd, stationid=estacion, datatypeid=type, startdate=ssDate, enddate=ffDate , limit=366, token=config$token)
      #Verify that available information exist
      if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
        if ( config$isgraphic ){
          gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
        }else{
          msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".", sep="" )
          cat( msg )
        }

        colnames( valores ) <- station$id[1:(i-1)]
        RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
        RPostgres::dbDisconnect( conn )
        return(tableName) 

      } 

      weatherYear<- weatherVar$data

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
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for statino ", estacion, " for variable type:  ", type, ".\t\n", sep="" )
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
	 
	      }else{ 
	      
	        lubridate::month( fDate ) <- lubridate::month( maxDate ) 
	        lubridate::day(   fDate ) <- lubridate::day(   maxDate ) 
	        ssDate         <- base::paste( sDate, "T00:00:00", sep="")
	        ffDate         <- base::paste( fDate, "T00:00:00", sep="")                      
	      
	        weatherVar<-rnoaa::ncdc(datasetid = ghcnd, stationid = estacion, datatypeid=type, startdate = ssDate, enddate= ffDate , limit=366, token=config$token)

	        #Verify that available information exist
      		if(length(as.character( weatherVar$meta$totalCount)) == 0 ){
                  if ( config$isgraphic ){
                    gWidgets::svalue(txt) <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\t\t\t\t", sep="" )
                  }else{
                    msg <- base::paste("Not Data Available for station ", estacion, " for variable type:  ", type, ".\t\n", sep="" )
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
	      
	    }

	    # TMAX = Maximum temperature (tenths of degrees C)	    
	    #promedio <- sprintf( "%.4f", mean( weatherYear$value/10 ) )     
	    
	    # Return year mean value  
	    #print( promedio )                                               

	    #NOAA specification of available data within a range often is not complete. 
	    #Individual dates are missing even within an specified range.
	    #SOLUTIONS:  
	    #1) Average from the available number of samples.  
	    #2) Interpolation when possible

	
	    if ( i == 1)
	    {
	    
	      intervalAsDate     <-  base::as.Date( weatherYear$date ) 
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
	        
	        #Main change from conn_rnooa_postgresql_writeOGR_locs_and_weatherData_v2 (v2)
		# is the following line. We replace average for Interpolated values.
	        #In v2 the vecPromedio variable hold the average value of the available data.
		#Currently as can be observed, it is replaced by NA that is replaced by an empty
		#cell when exported to a database with column holding a variable of type double
	        vecPromedio     <- rep( NA, sizeSubV1  )	
	        
		#Update allDatesHash values found in 'dataKeys' with NA 
	        hash::values( allDatesHash, keys = dateKeys) <- vecPromedio
	                 
	        valores <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )

	      }else{ valores  <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
	    }
	    else
	    {
	    
	      #cat("In ELSE section\n")
	      #print(estacion)
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
	        
	        #Main change from v2 is this line. We replace average for Interpolated values.
	        #In v2 the vecPromedio the current NA values were replaced instead by an average value.
	        vecPromedio      <- rep( NA, sizeSubV1  )
	        hash::values( allDatesHash, keys=dateKeys) <- vecPromedio
	        subValores       <- as.data.frame( hash::values( allDatesHash, keys=NULL ) )            
	        valores          <- data.frame( cbind( valores, subValores ) )	        

	      }else{ valores     <- data.frame( cbind( valores, intervalAsValue ) )  }#endIF/ELSE
	      
	    }
	        	    
            rm( stationDataHash )	
        
	  }#endFOR   (stick time series of different stations together)	
	  
	  colnames( valores ) <- station$id
#      print( tail( valores) )
      
      if (config$isgraphic){	
        w2   <- gWidgets::gwindow("Message: ", width=500)
        gp2  <- gWidgets::ggroup(container=w2, expand=TRUE)
        txt2 <- gWidgets::glabel("", expand=TRUE, container=gp2)	
        gWidgets::svalue( txt2 ) <- base::paste("Creating table ", tableName, ".\t\t\t\t\t", sep="")
      }else{
        msg <- base::paste("Creating table ", tableName, ".\t\t\t\t\t", sep="")
        cat(msg)
      }
      RPostgres::dbWriteTable( conn, tableName, as.data.frame( valores ) )
      Sys.sleep(4)
      if(config$isgraphic){
        gWidgets::gmessage("Check Postgres table!")    
      }else{cat("Check Postgres table!")}
	  
  }#endIF/ELSE
   
  RPostgres::dbDisconnect( conn )
  return(tableName) 

}









#' stretch_delay_latent_period() process weather information to produce stretch-delay of latent period
#'
#' This function uses the provided data frame with temperature time series to determine the 
#' completion of latent period for a given vector-borne disease with in a region. It creates
#' a table at the Postgres database. 
#'
#' @param geoid  FIPS number from census tables (tiger files) to determine a region
#' @param type Weather variable under investigation
#' @param span keyword to find the table with the desired data 
#' @param disease String with the name of the vector-borne disease 
#' @return Returns the name of the table containing the completion of the latent period 
#' @examples
#' geoid   <- '12087'
#' type    <- 'TMAX'
#' span <- '2'
#' disease <- 'dengue'
#' stretch_delay_latent_period( geoid, type, span, disease)
#'
#' @note In progres ....
#' @export
stretch_delay_latent_period <- function(geoid, type, span, disease){

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
    tableName <- base::paste(state, "_", geoid,"_ws_data_span_",span,"_avg_", sep="")

  }else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='",geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q2)
    county <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)

    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='",substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q3)
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    
    tableName <- base::paste(state, "_",county,"_",geoid,"_ws_data_span_",span,"_avg_", sep="")

  }

  varTable  <- base::tolower( tableName  )
  type      <- base::tolower(  type  )
  tableName <- base::paste( varTable, type, sep="")      
  tableName <- base::gsub(" ", "_", tableName)

  print(tableName)

  q4    <- base::paste("select r_table_exists('", tableName,"')", sep="")
  res   <- RPostgres::dbSendQuery(conn, q4)
  exists <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
  print(exists)
  if( exists ){
    degree <- 5 #For dengue, this order has the lowest error.
    tableDisease <- base::paste(tableName,"_",disease,sep="")
    print(tableDisease)
    q5    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
    res   <- RPostgres::dbSendQuery(conn, q5)
    exists <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    print(exists)
    if(  exists ){
       msg <- base::paste("\nDone - Table ", tableName, "  exists.\t\t\t\t\n", sep="")
       if ( config$isgraphic ){
         gWidgets::svalue(txt) <- msg
         #svalue(txt2) <- base::paste("Done -Table ", tableName, " exists.\t\t\t\t\n", sep="" )
         Sys.sleep(3)    
         gWidgets::gmessage("Double check your data table exsist.\n")
       }else{ cat(msg) }
         RPostgres::dbDisconnect(conn)
         return(paste("Table ", tableDisease," data table exists!",sep=""))    
    }else{

  	switch(disease,
	       dengue          = model <- dengue_model(tableName, disease, conn, degree),
	       malaria         = model <- malaria_model(tableName, disease, conn),
	       west_nile       = model <- west_nile_model(tableName, disease, conn),
	       chikungunya     = model <- chikungunya_model(tableName,disease, conn),
	       changas         = model <- changas_model(tableName, disease, conn),
	       la_crosse_virus = model <- la_crosse_virus_model(tableName,disease, conn)
 	       )
    }

  }else{

    RPostgres::dbDisconnect(conn)
    return(paste("Table ",tableName," data table DOES NOT exists!",sep=""))    
  }  
  RPostgres::dbDisconnect(conn)
  return(model)
}


#' dengue_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'dengue'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span)
#' tableDisease <- base::paste(tableData,"_dengue",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer( exists) ){ print("Exists!") }else{ dengue_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
dengue_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}



#' malaria_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'malaria'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' tableDisease <- base::paste(tableData,"_malaria",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer( exists) ){ print("Exists!") }else{ malaria_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
malaria_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}





#' west_nile_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'west_nile'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' tableDisease <- base::paste(tableData,"_west_nile",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer( exists) ){ print("Exists!") }else{ west_nile_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
west_nile_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}




#' chikungunya_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'chikungunya'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' tableDisease <- base::paste(tableData,"_chikungunya",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer( exists) ){ print("Exists!") }else{ chikungunya_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
chikungunya_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}



#' chagas_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'chagas'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' tableDisease <- base::paste(tableData,"_chagas",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer(exists) ){ print("Exists!") }else{ chagas_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
chagas_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  print(.O$real_temp)
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}







#' la_crosse_virus_model() habia una vez ...
#'
#' @param tableData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
#' config <- yaml::yaml.load_file( file )
#' driver <- "PostgreSQL"
#' drv    <- RPostgres::Postgres()
#' conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
#' ghcnd <- 'GHCND'
#' geoid <- '12087'
#' type  <- 'TMAX'
#' disease <- 'la_crosse_virus'
#' degree  <- '5'
#' stations <- as.data.frame( all_coor_ws( ghcnd, geoid, type) )
#' span <- '2'
#' tableData <- ws_metadata_span_2_pgdb( geoid, type, stations, span) 
#' tableDisease <- base::paste(tableData,"_la_crosse_virus",sep="") 
#' q    <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
#' res   <- RPostgres::dbSendQuery(conn, q)
#' exists <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if( as.integer( exists) ){ print("Exists!") }else{ la_crosse_virus_model(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
la_crosse_virus_model <- function(tableData, disease, conn, degree){
  library(RcppOctave)
  q1    <- base::paste("SELECT * FROM ", tableData, sep="")
  res   <- RPostgres::dbSendQuery(conn, q1)
  ws_data <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if(1){
    .O$num <- degree
    RcppOctave::o_source(text="\
	      eip_y1 = [25 18 13 12 7 7];\
	      rec_eip_y1 = 1./eip_y1;\
	      temp_eip = [24 26 27 30 32 35];\
	      min_temp_eip = min(temp_eip);\
	      max_temp_eip = max(temp_eip);\
	      eip_coeff_la = polyfit( temp_eip, eip_y1, num);\
	      r_eip = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) >= 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) <= 24 && real_temp(k,j) >= 0)\
		    eip(k,j) = 25;\
		  elseif(real_temp(k,j) <= 0)\
		    eip(k,j) = 0;\
		  else\
		    eip(k,j) = r_eip( real_temp(k,j) );\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  return(tableName)
}





