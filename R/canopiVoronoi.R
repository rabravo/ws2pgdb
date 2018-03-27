#' canopiVoronoi() creates a table with the polygones of a Voronoi tessellation that intersect a State/County.
#'
#' The function takes the name of a existing table made of at least to columns: a column with geometric points
#' and a column with unique identifiers for each element row in the table. The function produces a table in a 
#' Postgres database with geometries that form a Voronoi tessellation ( polygones ). The arguments needed for 
#' the function are two: a table's name and geoid number.
#'
#' @keywords Voronoi, tessellation, weather station, plr, pl/r, Postgres
#' @param tableName Table with geometric points used to generate Voronoi tessellation
#' @param geoid    The geoid is needed to identify the boundaries of intersection, 
#' where the generating points reside, that is in a State or a County.
#' 
#' @return Function returns the name of the table containing the polygones forming the Voronoi
#' or the value 1 when fail to find weather station information.
#' 
#' @examples
#' tableName <-all_coor_ws_2_pgdb( ghcnd  = 'GHCND',
#'                                 geoid  = '12087', 
#'                                 type   = 'PRCP', 
#'                                 suffix = 'example'
#'                               )
#' canopiVoronoi(tableName, geoid = '12087')
#' 
#' @note 
#' The output table name has the following form: st_co_geoid_ws_sufix_type_v_poly. 
#' "st" is the U.S. State name and  "co" is the U.S. County name. The "sufix" and "type" are 
#' specified by the user in the all_coor_ws_2_pgdb function. The following function assumes 
#' you have postgresql-9.x/-10.x, postgis-2.x, and postgresql-9.x-plr extension installed. Enable 
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
canopiVoronoi <- function(tableName, geoid) {

  file   <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
  config <- yaml::yaml.load_file( file )
  print("Downloading data and creating Postgres table ...\t\t\t\t ")
  if (!is.null( tableName)) {
    conn <- RPostgres::dbConnect( drv      = RPostgres::Postgres(), 
                                  host     = config$dbhost, 
                                  port     = config$dbport, 
                                  dbname   = config$dbname, 
                                  user     = config$dbuser, 
                                  password = config$pwd
                                )
      voronoiTableName  <- as.character(base::paste(tableName, "_v_poly", sep = ""))

      if (RPostgres::dbExistsTable(conn, voronoiTableName)) {
        txt <- base::paste("Done - Table ", voronoiTableName, " exists.\t\t\t\t\t", sep = "")
        cat(txt)
        RPostgres::dbDisconnect(conn)
        return(voronoiTableName)
      }# endIF

    # Intersection between the boundary of the county and the voronoi tesselation     
    if (as.integer(geoid) < 100) { 
		  tigerDB <- "cb_2013_us_state_20m" 
    } else { 
		  tigerDB <- "cb_2013_us_county_20m"
	}# endIF

    geoid        <- as.character(geoid)
    voronoiTable <- base::paste(" r_voronoi('",tableName , "', 'geom', 'ogc_fid')", sep = "")
    intersection <- base::paste(
	"with \
	 county  AS(\
		   SELECT ST_SetSRID(geom, 4326) as geom FROM ", tigerDB , " WHERE geoid= CAST( ", geoid ," As text) ),\
         voronoi AS(\
		   SELECT id, ST_SetSRID( polygon , 4326) as polygon FROM ", voronoiTable , "),\
	 output AS(\
		   SELECT id, ST_Intersection( v.polygon, t.geom) as v_polygon FROM voronoi as v, county as t )\
	 SELECT * INTO ", voronoiTableName , " FROM output;" , sep = "")

      # Invoke the system to execute the query
      # psql_query         <- base::paste("psql -U ", dbuser, " -d us_gisdb -c \"", intersection, "\"", sep="")
      # msg <- system(psql_query) 

      query   <- intersection
      res     <- RPostgres::dbSendQuery(conn, intersection)
      msg     <- data.frame(RPostgres::dbFetch(res))
      RPostgres::dbClearResult(res)
      RPostgres::dbDisconnect(conn) 
      return(voronoiTableName)
    } else {
       return('1')
    }# endIF/ELSE
}#endFUNCTION
