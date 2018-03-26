#' malaria_model_conn() habia una vez ...
#'
#' @param tMetaData is the name of the table containing weather data ( temperature )
#' @param disease is the name of the vector-borne disease (i.e dengue, malaria, chikungunya, etc)
#' @param conn is an open connectino to read/write to a pgdb
#' @param degree is the order at one wants to adjusts the polynomio (for dengue degree=5) has the lowest norm
#' @return returns the tail of the data that has been found. 
#' @examples
#' file      <- base::paste(Sys.getenv("HOME"), "/", "pg_config.yml", sep = "")
#' config    <- yaml::yaml.load_file( file )
#' drv       <- RPostgres::Postgres()
#' h         <- config$dbhost
#' p         <- config$dbport
#' d         <- config$dbname
#' u         <- config$dbuser
#' pwd       <- config$dbpwd
#' conn      <- RPostgres::dbConnect(drv, host = h, port = p, dbname = d, user = u, password = pwd)
#' stations  <- as.data.frame( all_coor_ws(ghcnd = 'GHCND', geoid = '12087', type = 'TMAX'))
#' tMetaData <- ws_metadata_span_2_pgdb(geoid = '12087', type = 'TMAX', stations, span = '2') 
#' disease  <- 'malaria'
#' tModelDisease <- base::paste(tMetaData, "_", disease, sep = "") 
#' q         <- base::paste("select r_table_exists('", tModelDisease, "')", sep = "")
#' res       <- RPostgres::dbSendQuery(conn, q)
#' exists    <- RPostgres::dbFetch(res)
#' RPostgres::dbClearResult(res)
#' if (as.integer(exists)) {
#'   print("Table exists!")
#' } else {
#'   malaria_model_conn(tMetaData, disease, conn, degree = '5')
#' }
#' RPostgres::dbDisconnect(conn)
#' @export
malaria_model_conn <- function(tMetaData, disease, conn, degree){
  library(RcppOctave)
  q1           <- base::paste("SELECT * FROM ", tMetaData, sep="")
  res          <- RPostgres::dbSendQuery(conn, q1)
  ws_data      <- data.frame(RPostgres::dbFetch(res), row.names=NULL)
  ws_data_temp <- data.matrix(ws_data, rownames.force=NA)
  RPostgres::dbClearResult(res)
  .O$real_temp <- ws_data_temp
  if (1) {
    # .O$num <- degree
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
  tableName <- paste(tMetaData, "_", disease, sep = "")
  RPostgres::dbWriteTable(conn, tableName, as.data.frame(.O$eip))
  o_clear(all = TRUE)
  return(tableName)
}
