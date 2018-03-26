#' createVoronoi() creates a Voronoi tessellation
#'
#' This function creates a table with polygone geometries of a Voronoi tessellation in a Postgres database.
#'
#' @param tableName The name of existing table within the Postgres database.
#' @param the_geom The column name with the geometries of the generating points.
#' @param ogc_fid  The column name with the unique identifiers for each row element (point).
#' @param scale The percentage of buffer distance (how far the expand outward) 
#' @return The function returns the name of the voronoi table that has been created. 
#' 
#' @examples
#' tableName<- all_coor_ws_2_pgdb(ghcnd='GHCND', geoid='40', type='TMAX', suffix='voronoi_example')
#' createVoronoi(tableName, the_geom = "geom", ogc_fid = "ogc_fid", scale = 0.25)
#'
#' @note In https://gist.github.com/djq/4714788 you will find the query code to be loaded into the database. 
#' The r_voronoi(text,text,text) function is needed to create the Voronoi tessellation. 
#' createVoronoi() function exploits this code, but a small modification was required. 
#' The buffer_distance has been made flexible in contrast to the original code that sets the buffer distance to 10%. 
#' The r_voronoi(text,text,text) has been replaced by the r_voronoi_scale(text,text,text,float), 
#' and TYPE was renamed as the r_voronoi_scale_type. In the original code, search for the section with the comment 
#' "calculate an approprate buffer distance (~10%):" and replace (0.10) for (arg4). That should suffice 
#' to make createVoronoi() to work as explained in the example.
#' 
#' @export
createVoronoi <- function(tableName, the_geom, ogc_fid, scale) {

  file   <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
  config <- yaml::yaml.load_file(file)
  print("Downloading data and creating Postgres table ...\t\t\t\t ")
  if (!is.null(tableName)) {
    conn      <- RPostgres::dbConnect( drv = RPostgres::Postgres(), 
                                       host     = config$dbhost, 
                                       port     = config$dbport, 
                                       dbname   = config$dbname, 
                                       user     = config$dbuser, 
                                       password = config$dbpwd
                                     )
    voronoiTableName  <- as.character( base::paste( tableName, "_v_poly", sep = ""))

    if (RPostgres::dbExistsTable(conn, voronoiTableName)) {
      txt <- base::paste("Done - Table ", voronoiTableName, " exists.\t\t\t\t\t", sep = "")
      cat(txt)
      RPostgres::dbDisconnect(conn)
      return( voronoiTableName )
    }# endIF

    query <-base::paste(" select * into ", voronoiTableName ," from r_voronoi_scale('", tableName , "','" , the_geom , "', '", ogc_fid , "',", scale, " );", sep = "")
    res <- RPostgres::dbSendQuery(conn, query)
    msg <- data.frame(RPostgres::dbFetch(res))   
    RPostgres::dbClearResult(res)
    RPostgres::dbDisconnect(conn)
    return(voronoiTableName)

  } else { return('1') }

}#endFUNCTION
