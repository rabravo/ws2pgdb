#' createVoronoi() creates a Voronoi tessellation
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