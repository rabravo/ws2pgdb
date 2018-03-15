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
#' span    <- '2'
#' disease <- 'dengue'
#' stretch_delay_latent_period( geoid, type, span, disease)
#'
#' @note In progres ....
#' @export
stretch_delay_latent_period <- function(geoid, type, span, disease){



  #Verifies whether the model has been implemented
  disease   <- base::tolower( disease )
  flag <- 0;
  if( nchar(disease)  ){
    switch(disease,
         dengue          = flag <-1,
         malaria         = flag <-1,
         west_nile       = flag <-1,
         chikungunya     = flag <-1,
         chagas          = flag <-1,
         la_crosse_virus = flag <-1
         )
  }
  if(!flag){
    return( base::paste("No model of '",disease,"' disease implemented yet! :(",sep="") )    
  }

  # Verifies whether the model has been implemented

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  # Test whether the user input a county or state
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

  }# endIF/ELSE

  varTable  <- base::tolower( tableName  )
  type      <- base::tolower(  type  )
  disease   <- base::tolower( disease )
  tableName <- base::paste( varTable, type, sep="")      
  tableName <- base::gsub(" ", "_", tableName)

  q4     <- base::paste("select r_table_exists('", tableName,"')", sep="")
  res    <- RPostgres::dbSendQuery(conn, q4)
  exists <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  if ( exists ) {

    degree <- 5 #For dengue, this order has the lowest error.
    tableDisease <- base::paste(tableName,"_",disease,sep="")
    q5     <- base::paste("select r_table_exists('", tableDisease,"')", sep="")
    res    <- RPostgres::dbSendQuery(conn, q5)
    exists <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)

    if ( exists ) {
       msg <- base::paste("\nDone - Table ", tableName, "  exists.\t\t\t\t\n", sep="")
       cat(msg)
       RPostgres::dbDisconnect(conn)
       return(paste("Table ", tableDisease," data table exists!",sep=""))    

    } else {

  	switch(disease,
	       dengue          = model <- dengue_model_conn( tableName, disease, conn, degree),
	       malaria         = model <- malaria_model_conn( tableName, disease, conn, degree),
	       west_nile       = model <- west_nile_model_conn( tableName, disease, conn, degree),
	       chikungunya     = model <- chikungunya_model_conn( tableName, disease, conn, degree),
	       chagas          = model <- chagas_model_conn( tableName, disease, conn, degree),
	       la_crosse_virus = model <- la_crosse_virus_model_conn( tableName, disease, conn, degree)
 	       )
    }# endIF/ELSE


  } else {

    RPostgres::dbDisconnect(conn)
    return(paste("Table ",tableName," needed for the model DOES NOT exist!",sep=""))    
  }# endIF/ELSE  

  RPostgres::dbDisconnect(conn)
  return(model)
}

