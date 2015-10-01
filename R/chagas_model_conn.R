#' chagas_model_conn() habia una vez ...
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
#' if( as.integer(exists) ){ print("Exists!") }else{ chagas_model_conn(tableData, disease, conn, degree) }
#' RPostgres::dbDisconnect(conn)
#' @export
chagas_model_conn <- function(tableData, disease, conn, degree){
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
