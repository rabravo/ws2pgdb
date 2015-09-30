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
  disease   <- base::tolower( disease )
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
	       malaria         = model <- malaria_model(tableName, disease, conn, degree),
	       west_nile       = model <- west_nile_model(tableName, disease, conn, degree),
	       chikungunya     = model <- chikungunya_model(tableName,disease, conn, degree),
	       chagas          = model <- chagas_model(tableName, disease, conn, degree),
	       la_crosse_virus = model <- la_crosse_virus_model(tableName,disease, conn, degree)
 	       )
    }


  }else{

    RPostgres::dbDisconnect(conn)
    return(paste("Table ",tableName," needed for the model DOES NOT exist!",sep=""))    
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
#' @return returns the table name with evaluation of the model over the available data. 
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
	      dengue_m = vectorize(inline(char(polyout(eip_coeff_la, 'T'))));\
\
	      for j=1:columns(real_temp)\
		for k=1:rows(real_temp)\
		  if (real_temp(k,j) > 32)\
		    eip(k,j) = 7;\
		  elseif( real_temp(k,j) >= 24 && real_temp(k,j) <= 32)\
		    eip(k,j) = dengue_m( real_temp(k,j) );\
		  elseif( real_temp(k,j) >= 16.4 && real_temp(k,j) < 24 )\
		    eip(k,j) = 25;\
		  else\
		    eip(k,j) = 0.0;\
		  endif\
	        endfor\
	     endfor\ 
	   ")

  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  o_clear(all=TRUE)
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
    #.O$num <- degree
    RcppOctave::o_source(text="\
	      malaria_m = vectorize(inline(char('0.71 * (T - 21 ) + 4.7')));\
              for j=1:columns(real_temp)\
                for k=1:rows(real_temp)\
                  if (real_temp(k,j) > 20 && real_temp(k,j) <= 31)\
                    eip(k,j) = malaria_m( real_temp(k,j) );\
                  else\
                    eip(k,j) = 0;\
                  endif\
                endfor\
             endfor\
	    ")
  }
  tableName <- paste(tableData,"_",disease, sep="")
  RPostgres::dbWriteTable( conn, tableName, as.data.frame( .O$eip ) )
  o_clear(all=TRUE)
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
