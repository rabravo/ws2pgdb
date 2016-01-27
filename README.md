# ws2pgdb
Package contains several functions that retrieve, filter, and store NOAA data in a local/remote Postgres database. 
One useful function is that that takes weather station location in a region and constructs a Voronoi tessellation over that region (you will need a GIS software to visualize the output which is a set of geometries/polygones). 
Another useful function within the package is that that iteratively request several years of data from the NOAA digital warehouse. Since NOAA permits only one year of data requested at a time, via the rnoaa package. It is particular useful when many years of information are needed. 



#PLR/SQL and PLPGSQL
The following function enables the communication between the vector-borne simulator and the pgsql ( database ).

Copy, paste, and execute these queries on the pgsql server.

```
CREATE OR REPLACE FUNCTION public.r_all_coor_ws(text, text, text)
  RETURNS SETOF all_coor_ws_type AS
$BODY$ 

ghcnd <- arg1
geoid <- arg2
type  <- arg3

ws <- ws2pgdb::all_coor_ws( ghcnd, geoid, type)
return(ws[,c("id","longitude","latitude")])
$BODY$
  LANGUAGE plr;

CREATE OR REPLACE FUNCTION public.r_column_names(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
#i.e. SELECT r_column_names('48061','10','malaria')

geoid  <- as.character(arg1)
span   <- as.character(arg2)
disease<- arg3
num    <- as.integer(arg4)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax",sep="")
t2  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax_",disease,sep="")

q2 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")
wsName <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q2 ) ))
wsName <- wsName[2:length(wsName[,1]),]

q3 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t2, "' ) as g", sep="")

wsModel <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q3 ) ))
wsTempAndModel <- data.frame( c( wsName[num], wsModel[num] ) )

return(wsTempAndModel)

$BODY$
  LANGUAGE plr;





CREATE OR REPLACE FUNCTION public.r_county_centroid(text, text)
  RETURNS SETOF double precision AS
$BODY$ 

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )

#geoid is the fips number. This function only accept fips for counties (5 digits)
geoid   <- arg1
#coord takes either 'lat' or 'lon' to indicate [lat]itude or [lon]gitude respectively
pos    <- arg2

driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

if ( pos == 'lat'){
  q1   <- base::paste("SELECT ST_Y( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lat FROM ( SELECT  ST_Dump( ST_CollectionHomogenize( geom ) ) as geom  FROM cb_2013_us_county_20m WHERE geoid = '", geoid ,"') AS g", sep="")
  res  <-RPostgres::dbSendQuery(conn, q1)
  coord  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}
if ( pos == 'lon'){
  q1   <- base::paste("SELECT ST_X( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lon FROM ( SELECT  ST_Dump( ST_CollectionHomogenize( geom ) ) as geom  FROM cb_2013_us_county_20m WHERE geoid = '", geoid ,"') AS g", sep="")
  res  <-RPostgres::dbSendQuery(conn, q1)
  coord  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}
RPostgres::dbDisconnect(conn)

return(coord)

$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_create_canopi_voronoi(text, text)
  RETURNS text AS
$BODY$ 

#'geoid' is the fips number of the county where the tessellations will intersect
geoid      <- arg1

#'tableName' is the table containing the points that will produce the tessellations.
#The points must be within the boundaries of the county under investigation or
#near enought to perform a geometric intersection with the tessellation polygones. 
#The table must have the following columns, 'geom', 'ogc_fid' where geom is the
#collection of location of points (ws) and ogc_fid is the unique identifier of the points
#Usually, the tables named state_county_fips_ws_subset_metadata_span_num_type or
#state_county_fips_ws_subset_metadata_span_num_type contain these columns
tableName  <- arg2

  return(ws2pgdb::canopiVoronoi(tableName, geoid))

$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_create_county_info(text, text, text)
  RETURNS text AS
$BODY$
  #i.e. SELECT r_create_county_info('12087', 'TMAX', '10') 
  geoid <- arg1
  type  <- arg2
  span  <- arg3
  ghcnd <- 'GHCND'


  pgfile   <- base::paste(Sys.getenv("PWD"), "/","pg_config.yml", sep="")
  config   <- yaml::yaml.load_file( pgfile )
  drv    <- RPostgres::Postgres()

  #To override auth, provide your passwd via the .pgpass (see postgresql documentation)
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  stations       <-  ws2pgdb::all_coor_ws( ghcnd, geoid, type) 
  ws_metadata    <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 


  q1             <- base::paste("SELECT r_create_tiger_tracts_table('",geoid ,"')",sep="")
  res            <- RPostgres::dbSendQuery(conn, q1)
  tigertableName <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)      
  #tigertableName <- pg.spi.exec( sprintf("%1$s",q1) )
  table_cluster  <- paste( tigertableName, "_clustered_by_nearest_ws", sep="")

  # tractce) Build the distance matrix from the subregion's centroid to all weather stations
  # geoid: tract geoid; path2Hub: text form of geom; 
  # geom As poly: tract polygone
  # geom: line between the a) a tract centroid, b) a weather station;
  # dist: length of the geometry
  # name: name of weather station

  q2             <- base::paste("SELECT r_create_midas_synth_hh_table('",geoid ,"')",sep="")
  res            <- RPostgres::dbSendQuery(conn, q2)
  midastableName <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)  
  #midastableName <- pg.spi.exec( sprintf("%1$s",q2) )
  temp           <- tigertableName
  midas_pop      <- base::gsub("tiger_tracts", "midas_pop", temp)
  midas_pop_clustered_by_nearest_ws <- base::paste(midas_pop,"_clustered_by_nearest_ws",sep="")

  q3_check       <- base::paste("SELECT r_table_exists('",table_cluster,"')", sep="")
  res            <- RPostgres::dbSendQuery(conn, q3_check)
  midasExist     <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)  

  #$if( base::as.integer( pg.spi.exec( sprintf("%1$s",q3_check) ) ) ){
  if( midasExist ){
    print("Exists!");
  }else{

    q3 <- base::paste("\
    with\
      tractce As(\
        SELECT tract.geoid10 as geoid, tract.the_geom as geom, ST_MakeLine(ST_Centroid( tract.the_geom ), coord.geom ) as geomLine, ST_Length( ST_MakeLine(ST_Centroid( tract.the_geom ), coord.geom ) ) as dist, coord.name, ST_Centroid( tract.the_geom ) as centroid FROM ", tigertableName, " tract, ", ws_metadata, " coord\
      ), \
      geoidDistance As(\
        SELECT geoid, min( dist ) as min_dist FROM tractce GROUP BY geoid\
      ),\
      line2Hub As(\
    SELECT tractce.geoid, tractce.geom as poly, ST_AsText(tractce.geomLine), tractce.centroid, name, min_dist FROM tractce, geoidDistance WHERE geoidDistance.min_dist = dist ORDER BY tractce.name \
      ),\
      cluster_ws As(\
        SELECT name, ST_MULTI( ST_UNION(poly) ) FROM line2Hub GROUP BY name\
      )\
      SELECT * INTO ", table_cluster , " FROM cluster_ws ",  sep="")

    res        <- RPostgres::dbSendQuery(conn, q3)
    q3_out     <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)      
    #q3_out <- pg.spi.exec( sprintf("%1$s",q3) )
  }


  q4_check          <- base::paste("SELECT r_table_exists('",midas_pop_clustered_by_nearest_ws,"')", sep="")
  res               <- RPostgres::dbSendQuery(conn, q4_check)
  midasClusterExist <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)        
  
  #if( as.integer( pg.spi.exec( sprintf("%1$s",q4_check) ) ) ){
  if( midasClusterExist ){
  
    print("Exists!");
    
  }else{
  
    q4 <- base::paste("
    with\
      tractce As(\
    SELECT ogc_fid as ws_id, tract.geoid10 as geoid, tract.the_geom as geom, ST_MakeLine(ST_Centroid( tract.the_geom), coord.geom ) as geomLine, ST_Length( ST_MakeLine(ST_Centroid( tract.the_geom), coord.geom ) ) as dist, coord.name, ST_Centroid( tract.the_geom ) as centroid FROM ", tigertableName, " tract, ", ws_metadata , " coord ORDER BY dist ASC\
      ),\
      geoidDistance As(\
    SELECT geoid, min( dist ) as dist FROM tractce GROUP BY geoid --ORDER BY dist\
      ),\
      line2Hub As(\
    SELECT  DISTINCT ON (gd.geoid) ws_id, t.geomLine, t.geom as poly, t.name, gd.geoid, gd.dist FROM geoidDistance gd, tractce t WHERE gd.geoid = t.geoid  AND abs(gd.dist - t.dist) < 0.0000001\
      ),\
      ind_per_bg As(\
        SELECT stcotrbg As geoid, SUM(hh_size) As hh FROM ", midastableName," GROUP BY stcotrbg ORDER BY stcotrbg\
      ),\
      ind_per_tract As(\
        SELECT substring( geoid for 11 ) As geoid, SUM(hh) As hh FROM ind_per_bg GROUP BY substring( geoid for 11) ORDER BY substring( geoid for 11)\
      ),
      syn_pop As(\
        SELECT ws_id, t.name, t.geoid, t.poly, midas.hh FROM ind_per_tract midas, line2Hub t WHERE midas.geoid = t.geoid\
      ),\
      cluster_pop As(\
        SELECT ws_id, name, sum(hh) as hh, ST_MULTI( ST_UNION(poly) ) as poly FROM syn_pop GROUP BY name, ws_id\
      )\
      SELECT * INTO ", midas_pop_clustered_by_nearest_ws, " FROM cluster_pop ORDER BY ws_id", sep="")

    res      <- RPostgres::dbSendQuery(conn, q4)
    q4_out   <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    #q4_out <- pg.spi.exec( sprintf("%1$s",q4) )
  }

  ws2pgdb::ws_data_avg_span_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
  ws2pgdb::ws_data_na_span_2_pgdb(  ghcnd, geoid, type, span, ws_metadata )


  return(midas_pop_clustered_by_nearest_ws)

$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_create_midas_synth_hh_table(text)
  RETURNS text AS
$BODY$
pwd		<- base::Sys.getenv("PWD")
url		<- "http://www.epimodels.org/10_Midas_Docs/SynthPop/2010/counties/"
geoid		<- arg1
#geoid		<- "12087"

if( as.integer(geoid) < 100){
    
    q1    <- base::paste( "select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    state <- as.character( pg.spi.exec( sprintf( "%1$s", q3 ) ) )
    pre   <- base::paste(state, "_", sep="")

}else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    county<- as.character( pg.spi.exec( sprintf( "%1$s", q2 ) ) )
    st_geoid <- substr(geoid, 1, 2)
    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", st_geoid,"'", sep="")
    state <- as.character( pg.spi.exec( sprintf( "%1$s", q3 ) ) )
    pre   <- base::paste(state, "_", county,"_",sep="")
}

pre       <- tolower( pre )
pre       <- gsub(" ", "_", pre)

tableName  <- paste(pre, geoid,"_midas_synth_hh", sep="")
q4 <- base::paste("SELECT r_table_exists('", tableName,"')", sep ="")
tableExist  <- as.integer( pg.spi.exec( sprintf( "%1$s", q4 ) ) )

if ( tableExist ){
  return(tableName)
}else{

  debug <- as.integer( pg.spi.exec( sprintf( "SELECT r_create_synth_hh_table_template('%1$s')", tableName ) ) )
  print(debug)
  
  file		<- base::paste("2010_ver1_",geoid,sep="")
  pfile		<- base::paste(pwd,"/","2010_ver1_",geoid,sep="")
  extractedFile	<- base::paste(file, "_synth_households.txt", sep="")
  updatedFile	<- base::paste(pfile, "_synth_hh.csv", sep="")
  zipFile	<- base::paste(pfile,".zip", sep="")
  download	<- base::paste(url, file, ".zip",sep="")
  downloader::download( download , dest=zipFile, mode="wb")
  utils::unzip(paste(pfile,".zip",sep=""), file=extractedFile) 
  input		<- utils::read.csv(file=extractedFile, head=TRUE,sep=",")
  out 		<- input[c("stcotrbg", "hh_race", "hh_income","hh_size", "hh_age","longitude","latitude")]
  utils::write.csv(out,file=updatedFile, row.names=FALSE)
  q4 <-pg.spi.exec( sprintf( "COPY \"%1$s\" FROM '%2$s' DELIMITER ',' CSV HEADER;", tableName, updatedFile) )
  q5 <- paste("rm ",zipFile," ",updatedFile," ",extractedFile,sep="")
  system(q5)
  return(tableName)
}
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_create_synth_hh_table_template(arg1 text)
  RETURNS text AS
$BODY$
DECLARE
c INT;
BEGIN 
  EXECUTE 'CREATE TABLE "'
	|| arg1 
	||'" (stcotrbg  CHAR(14), hh_race SMALLINT, hh_income DOUBLE PRECISION, hh_size SMALLINT, hh_age SMALLINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION );'; 
	RETURN arg1; 
END;
$BODY$
  LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION public.r_create_table(var text)
  RETURNS integer AS
$BODY$
DECLARE
c INT;
BEGIN 
  EXECUTE 'CREATE TABLE "'
	|| var 
	||'" (stcotrbg  CHAR(14), hh_race SMALLINT, hh_income DOUBLE PRECISION, hh_size SMALLINT, hh_age SMALLINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION );'; 
	RETURN 0; 
	EXCEPTION 
		WHEN SQLSTATE '42P07' THEN RETURN 1; 
END;

$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.r_create_tiger_tracts_table(text)
  RETURNS text AS
$BODY$

pgfile   <- base::paste(Sys.getenv("PWD"), "/","pg_config.yml", sep="")
print(pgfile)
pwd     <- base::Sys.getenv("PWD")
print(pwd)

config   <- yaml::yaml.load_file( pgfile )

url     <- "ftp://ftp2.census.gov/geo/pvs/tiger2010st/"
geoid       <- arg1
geoid      <- "12087"

driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()

#To override auth, provide your passwd via the .pgpass (see postgresql documentation)
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


if( as.integer(geoid) < 100){

    q1    <- base::paste( "select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q1)
    state <- as.character(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)    
    #state <- as.character( pg.spi.exec( sprintf( "%1$s", q1 ) ) )
    pre   <- base::paste(state, "_", sep="")

}else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q2)
    county<- as.character(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)    
    #county<- as.character( pg.spi.exec( sprintf( "%1$s", q2 ) ) )

    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q3)
    state <- as.character(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)        
    #state <- as.character( pg.spi.exec( sprintf( "%1$s", q3 ) ) )
    pre   <- base::paste(state, "_", county,"_",sep="")
}

state     <- gsub(" ", "_", state)
pre       <- gsub(" ", "_", pre)
pre       <- tolower( pre )

file      <- base::paste("tl_2010_",geoid,"_tract10",sep="")
ext       <- ".zip"
url   <- base::paste("ftp://ftp2.census.gov/geo/pvs/tiger2010st/",substr(geoid, 1, 2),"_",state,"/",geoid,"/",sep="")

tableName    <- base::paste(file, sep="")
pretableName <- base::paste(pre, geoid, "_tiger_tracts",sep="")

q4 <- base::paste("SELECT r_table_exists('", pretableName,"')", sep ="")
res   <- RPostgres::dbSendQuery(conn, q4)
tableExist <- as.integer( RPostgres::dbFetch(res) )
RPostgres::dbClearResult(res)    

if ( tableExist ){
  return(pretableName)
}else{
  pfile      <- base::paste(pwd,"/",file,sep="")
  download   <- base::paste(url, file, ext, sep="")
  zipFile    <- base::paste(pwd,"/temp/",file, ext, sep="")
  downloader::download( download , dest=zipFile, mode="wb")
  dir        <- base::paste(pwd,"/","temp",sep="")
  utils::unzip(zipFile, overwrite = 'TRUE', exdir=dir)
  ext        <-".shp"
  q5  <- base::paste("shp2pgsql -c -s 4269 -g the_geom -W latin1 ",pwd, "/", "temp/", file, ext, " public.", pretableName, " " ,config$dbname," > ",pwd,"/script.sql", sep="")
  system(q5)
  q6  <- base::paste("psql -d ",config$dbname," -U ", config$dbuser, " -p ",config$dbport," -q --file='", pwd,"/script.sql'",sep="")
  system(q6)
  q7 <- paste("rm ", pwd, "/", "temp/*",sep="")
  system(q7)

  return(pretableName)
}

$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_gen_data_avg_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$ 
# i.e. SELECT r_gen_data_avg_span_2_pgdb('GHCND','12087','TMAX','10')
ghcnd <- arg1
geoid <- arg2
type  <- arg3
stations <- as.data.frame( ws2pgdb::all_coor_ws( ghcnd, geoid, type) )
span  <- arg4
ws_metadata  <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
ws_data <- ws2pgdb::ws_data_avg_span_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
return(ws_data)
$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_gen_data_na_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$ 
# i.e. SELECT r_gen_data_na_span_2_pgdb('GHCND','12087','TMAX','10')
ghcnd <- arg1
geoid <- arg2
type  <- arg3
stations <- as.data.frame( ws2pgdb::all_coor_ws( ghcnd, geoid, type) )
span  <- arg4
ws_metadata  <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
ws_data <- ws2pgdb::ws_data_na_span_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
return(ws_data)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_gen_data_span_avg_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$ 
# i.e. SELECT r_gen_data_span_avg_2_pgdb('GHCND','12087','TMAX','10')
ghcnd <- arg1
geoid <- arg2
type  <- arg3
stations <- as.data.frame( ws2pgdb::all_coor_ws( ghcnd, geoid, type) )
span  <- arg4
stations  <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
ws_metadata <- ws2pgdb::ws_data_span_avg_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
return(ws_metadata)
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_gen_data_span_na_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$ 
# i.e. SELECT r_gen_data_span_na_2_pgdb('GHCND','12087','TMAX','10')
ghcnd <- arg1
geoid <- arg2
type  <- arg3
stations <- as.data.frame( ws2pgdb::all_coor_ws( ghcnd, geoid, type) )
span  <- arg4
stations  <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 
ws_metadata <- ws2pgdb::ws_data_span_na_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
return(ws_metadata)
$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_gen_model(text, text, text, text)
  RETURNS text AS
$BODY$
#i.e. SELECT r_gen_model('12087','TMAX','10','dengue')

library(RcppOctave)
geoid   <- arg1
type    <- arg2
span    <- arg3
disease <- arg4

return(ws2pgdb::stretch_delay_latent_period(geoid, type, span, disease))

$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_get_octave_version()
  RETURNS text AS
$BODY$
  return(RcppOctave::.CallOctave('version'))
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_get_population_size(text)
  RETURNS integer AS
$BODY$ 

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
geoid  <- arg1
q1     <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
res    <- RPostgres::dbSendQuery(conn, q1)
nameTable  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

midasTable    <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws" ,sep="")
q2     <- base::paste("SELECT r_table_exists('", midasTable,"')",sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
exists  <- as.integer(RPostgres::dbFetch(res))

if ( exists ){
  q3     <- base::paste("SELECT SUM(hh) FROM ", midasTable, sep="")
  res    <- RPostgres::dbSendQuery(conn, q3)
  total  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}else{
  midasTable    <- base::paste(nameTable, "midas_synth_hh" ,sep="")
  q3     <- base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep="")
  res    <- RPostgres::dbSendQuery(conn, q3)
  total  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
  #return(base::paste(midasTable, " does not exists ,sep="") )
}

RPostgres::dbDisconnect(conn)
  return(total)
$BODY$
  LANGUAGE plr;






CREATE OR REPLACE FUNCTION public.r_get_pos(text, text)
  RETURNS SETOF double precision AS
$BODY$ 

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )

#geoid is the fips number. This function only accept fips for counties (5 digits)
geoid <- arg1
pos   <- arg2

#Weathe variable
type  <- 'TMAX'
#Look-back in time
span  <- '3'

driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

if ( pos == 'lat'){

  q1   <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
  res  <-RPostgres::dbSendQuery(conn, q1)
  tableName  <- data.frame(RPostgres::dbFetch(res))
  type <- tolower(type)
  tableName  <- paste(tableName,"ws_metadata_span_",span,"_",type,sep="")
  RPostgres::dbClearResult(res)
  
  q2   <- base::paste("SELECT ST_Y(lat.geom) FROM ", tableName, " as lat", sep="")
  res  <-RPostgres::dbSendQuery(conn, q2)
  coord  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}
if ( pos == 'lon'){

  q1   <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
  res  <-RPostgres::dbSendQuery(conn, q1)
  tableName  <- data.frame(RPostgres::dbFetch(res))
  type <- tolower(type)
  tableName  <- paste(tableName,"ws_metadata_span_",span,"_",type,sep="")
  RPostgres::dbClearResult(res)

  q2   <- base::paste("SELECT ST_X(lon.geom) FROM ", tableName, " as lon", sep="")
  res  <-RPostgres::dbSendQuery(conn, q2)
  coord  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}
RPostgres::dbDisconnect(conn)

return(coord)

$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_get_r_version()
  RETURNS text AS
$BODY$ 
  return(getRversion())
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_get_tract_size(text, text)
  RETURNS integer AS
$BODY$ 

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

geoid  <- as.character(arg1)
wsNum  <- as.integer(arg2)
if (wsNum == 0){
  return(0)
}

q1     <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
res    <- RPostgres::dbSendQuery(conn, q1)
nameTable  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

midasTable    <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws " ,sep="")
q2     <- base::paste("SELECT r_table_exists('", midasTable,"')",sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
exists  <- as.integer(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

if ( exists ){
  #Fail badly if table entry does not exist
  wsNum  <- wsNum - 1
  str1   <- base::paste("SELECT ws_id FROM ", midasTable," LIMIT 1 OFFSET ", wsNum , sep="")
  res    <- RPostgres::dbSendQuery(conn,str1)
  rowNum  <- data.frame(RPostgres::dbFetch(res))

  q3     <- base::paste("SELECT hh FROM ", midasTable,"WHERE ws_id = ",rowNum, sep="")
  res    <- RPostgres::dbSendQuery(conn, q3)
  total  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}else{
  total <- 0
}
  
  RPostgres::dbDisconnect(conn)
  return(total)
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_model_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_model_colname('48061','10','malaria','4')

geoid  <- as.character(arg1)
span   <- as.character(arg2)
disease<- arg3
wsNum    <- as.integer(arg4)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax_",disease,sep="")
q1 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")

wsModel <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q1 ) ))
wsModel <- wsModel[1:length(wsModel[,1]),]
ws      <- base::paste("\"",wsModel[wsNum],"\"",sep="")

return(ws)

$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_model_values(text, text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_model_values('48061','10','malaria','4','TMIN')

geoid  <- base::as.character(arg1)
span   <- base::as.character(arg2)
disease<- base::as.character(arg3)
wsNum  <- base::as.integer(arg4)
type   <- base::as.character(arg5)
type   <- base::tolower(type)

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_",type,"_",disease,sep="")
q1 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")

wsModel <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q1 ) ))
wsModel <- wsModel[1:length(wsModel[,1]),]
ws      <- base::paste("\"",wsModel[wsNum],"\"",sep="")
q2      <- base::paste("SELECT ",ws," FROM ", t1 , sep="")
res     <- RPostgres::dbSendQuery(conn, q2)
wsValue <- data.frame(RPostgres::dbFetch(res))

RPostgres::dbClearResult(res)
RPostgres::dbDisconnect(conn)

return(wsValue)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_name_2_fips(text, text)
  RETURNS text AS
$BODY$ 

#First arguments needs the Official name that is it has to include Capitalization
#Second arguments can take the value of 'st' for state, or 'co' for county
#Return one or more fips 
#i.e. SELECT r_name_2_fips('Monroe', 'co')

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
name   <- arg1
key    <- arg2

driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if( key == 'st'){

    q1   <- base::paste("select GEOID from cb_2013_us_state_20m where NAME='",name,"'", sep="")
    res  <- RPostgres::dbSendQuery(conn, q1)
    nom  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
  }
  if( key == 'co'){

    q1   <- base::paste("select GEOID from cb_2013_us_county_20m where NAME='",name,"'", sep="")
    res  <-RPostgres::dbSendQuery(conn, q1)
    nom  <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
  }

  return(nom)

$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_read_population_size(text)
  RETURNS integer AS
$BODY$ 

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
geoid  <- arg1
q1     <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
res    <- RPostgres::dbSendQuery(conn, q1)
nameTable  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

midasTable    <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws" ,sep="")
q2     <- base::paste("SELECT r_table_exists('", midasTable,"')",sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
exists  <- as.integer(RPostgres::dbFetch(res))

if ( exists ){
  q3     <- base::paste("SELECT SUM(hh) FROM ", midasTable, sep="")
  res    <- RPostgres::dbSendQuery(conn, q3)
  total  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
}else{
  midasTable    <- base::paste(nameTable, "midas_synth_hh" ,sep="")
  q3     <- base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep="")
  res    <- RPostgres::dbSendQuery(conn, q3)
  total  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
  #return(base::paste(midasTable, " does not exists ,sep="") )
}

RPostgres::dbDisconnect(conn)
  return(total)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_read_ws(text, text, text)
  RETURNS SETOF text AS
$BODY$ 
# SELECT r_read_ws('12087', 'TMAX', '10')
geoid <- arg1
type  <- arg2
span  <- arg3

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
geoid  <- arg1
q1     <- base::paste("SELECT r_table_prefix('",geoid,"')", sep="")
res    <- RPostgres::dbSendQuery(conn, q1)
nameTable  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

metainfoTable    <- base::paste(nameTable, "ws_metadata_span_",span,"_",type ,sep="")
q2     <- base::paste("SELECT name FROM ", metainfoTable, sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
ws  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)
RPostgres::dbDisconnect(conn)
  return(ws)
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_table_exists(var text)
  RETURNS integer AS
$BODY$
DECLARE
c INT;
BEGIN 
  EXECUTE 'SELECT * FROM '
	|| var
	||';'; 
	RETURN 1; 
	EXCEPTION 
		WHEN SQLSTATE '42P01' THEN RETURN 0; 
END;

$BODY$
  LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION public.r_table_prefix(text)
  RETURNS text AS
$BODY$ 

#i.e. SELECT r_table_prefix('12087')

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
geoid  <- arg1

driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if( as.integer(geoid) < 100){

    q1    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q1)
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state,"_",geoid,"_",sep="")

  }else{

    q2    <- base::paste("select NAME from cb_2013_us_county_20m where GEOID='", geoid,"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q2)
    county<- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    q3    <- base::paste("select NAME from cb_2013_us_state_20m where GEOID='", substr(geoid, 1, 2),"'", sep="")
    res   <- RPostgres::dbSendQuery(conn, q3)
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    tableName <- base::paste(state, "_",county,"_",geoid,"_", sep="")
  }
  
  varTable           <- tolower( tableName  )
  tableName          <-  gsub(" ", "_", varTable)
  tableName          <-  gsub("-", "_", varTable)


  return(tableName)

$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_typenames()
  RETURNS SETOF r_typename AS
$BODY$
  x <- ls(name = .GlobalEnv, pat = "OID")
  y <- vector()
  for (i in 1:length(x)) {y[i] <- eval(parse(text = x[i]))}
  data.frame(typename = x, typeoid = y)
$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_version()
  RETURNS SETOF r_version_type AS
$BODY$
  cbind(names(version),unlist(version))
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_voronoi(text, text, text)
  RETURNS SETOF r_voronoi_type AS
$BODY$
library(deldir)
# select the point x/y coordinates into a data frame
points <- pg.spi.exec(
sprintf(
"SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;",
arg1,
arg2
)
)
# calculate an appropriate buffer distance (~10%):
# Update: (~50%) for further operations with in POSTGRESQL
buffer_distance = (
(
abs(max(points$x) - min(points$x)) +
abs(max(points$y) - min(points$y))
) / 2
) * (0.50)
# get EWKB for the overall buffer of the convex hull for all points:
buffer_set <- pg.spi.exec(
sprintf(
"SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;",
arg1,
arg2,
buffer_distance
)
)
# the following use of deldir uses high precision and digits to prevent
# slivers between the output polygons, and uses a relatively large bounding
# box with four dummy points included to ensure that points in the
# peripheral areas of the dataset are appropriately enveloped by their
# corresponding polygons:
voro = deldir(
points$x,
points$y,
digits=22,
frac=0.00000000000000000000000001,
list(ndx=2,ndy=2),
rw=c(
min( points$x ) - abs( min( points$x ) - max( points$x) ),
max( points$x ) + abs( min( points$x ) - max( points$x) ),
min( points$y ) - abs( min( points$y ) - max( points$y) ),
max( points$y ) + abs( min( points$y ) - max( points$y) )
)
)
tiles = tile.list(voro)
poly = array()
id = array()
p = 1
# construct the outgoing WKT now
for (i in 1:length(tiles)) {
tile = tiles[[i]]
curpoly = "POLYGON(("
for (j in 1:length(tile$x)) {
curpoly = sprintf(
"%s %.6f %.6f,",
curpoly,
tile$x[[j]],
tile$y[[j]]
)
}
curpoly = sprintf(
"%s %.6f %.6f))",
curpoly,
tile$x[[1]],
tile$y[[1]]
)
# this bit will find the original point that corresponds to the current
# polygon, along with its id and the SRID used for the point geometry
# (presumably this is the same for all points)...this will also filter
# out the extra polygons created for the four dummy points, as they
# will not return a result from this query:
ipoint <- pg.spi.exec(
sprintf(
"SELECT %3$s AS id, st_intersection('SRID='||st_srid(%2$s)||';%4$s'::text,'%5$s') AS polygon FROM %1$s WHERE st_intersects(%2$s::text,'SRID='||st_srid(%2$s)||';%4$s');",
arg1,
arg2,
arg3,
curpoly,
buffer_set$ewkb[1]
)
)
if (length(ipoint) > 0) {
poly[[p]] <- ipoint$polygon[1]
id[[p]] <- ipoint$id[1]
p = (p + 1)
}
}
return(data.frame(id,poly))
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_voronoi_scale(text, text, text, double precision)
  RETURNS SETOF r_voronoi_scale_type AS
$BODY$
library(deldir)
# select the point x/y coordinates into a data frame
points <- pg.spi.exec(
sprintf(
"SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;",
arg1,
arg2
)
)
# calculate an appropriate buffer distance (~10%):
# Update: (~50%) for further operations with in POSTGRESQL
buffer_distance = (
(
abs(max(points$x) - min(points$x)) +
abs(max(points$y) - min(points$y))
) / 2
) * (arg4)
# get EWKB for the overall buffer of the convex hull for all points:
buffer_set <- pg.spi.exec(
sprintf(
"SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;",
arg1,
arg2,
buffer_distance
)
)
# the following use of deldir uses high precision and digits to prevent
# slivers between the output polygons, and uses a relatively large bounding
# box with four dummy points included to ensure that points in the
# peripheral areas of the dataset are appropriately enveloped by their
# corresponding polygons:
voro = deldir(
points$x,
points$y,
digits=22,
frac=0.00000000000000000000000001,
list(ndx=2,ndy=2),
rw=c(
min( points$x ) - abs( min( points$x ) - max( points$x) ),
max( points$x ) + abs( min( points$x ) - max( points$x) ),
min( points$y ) - abs( min( points$y ) - max( points$y) ),
max( points$y ) + abs( min( points$y ) - max( points$y) )
)
)
tiles = tile.list(voro)
poly = array()
id = array()
p = 1
# construct the outgoing WKT now
for (i in 1:length(tiles)) {
tile = tiles[[i]]
curpoly = "POLYGON(("
for (j in 1:length(tile$x)) {
curpoly = sprintf(
"%s %.6f %.6f,",
curpoly,
tile$x[[j]],
tile$y[[j]]
)
}
curpoly = sprintf(
"%s %.6f %.6f))",
curpoly,
tile$x[[1]],
tile$y[[1]]
)
# this bit will find the original point that corresponds to the current
# polygon, along with its id and the SRID used for the point geometry
# (presumably this is the same for all points)...this will also filter
# out the extra polygons created for the four dummy points, as they
# will not return a result from this query:
ipoint <- pg.spi.exec(
sprintf(
"SELECT %3$s AS id, st_intersection('SRID='||st_srid(%2$s)||';%4$s'::text,'%5$s') AS polygon FROM %1$s WHERE st_intersects(%2$s::text,'SRID='||st_srid(%2$s)||';%4$s');",
arg1,
arg2,
arg3,
curpoly,
buffer_set$ewkb[1]
)
)
if (length(ipoint) > 0) {
poly[[p]] <- ipoint$polygon[1]
id[[p]] <- ipoint$id[1]
p = (p + 1)
}
}
return(data.frame(id,poly))
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_ws_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_ws_colname('48061','10','malaria','4')

geoid  <- as.character(arg1)
span   <- as.character(arg2)
disease<- arg3
wsNum    <- as.integer(arg4)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax",sep="")

q1 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")
wsName <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q1 ) ))
wsName <- wsName[2:length(wsName[,1]),]
ws <- base::paste("\"",wsName[wsNum],"\"",sep="")

return(ws)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_ws_values(text, text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_ws_values('48061','10','TMAX','4')

geoid  <- as.character(arg1)
span   <- as.character(arg2)
type   <- as.character(arg4)
type   <- tolower(type)
wsNum  <- as.integer(arg4)

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_",type,sep="")

q1 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")
wsName <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q1 ) ))
wsName <- wsName[2:length(wsName[,1]),]
ws <- base::paste("\"",wsName[wsNum],"\"",sep="")
q2     <- base::paste("SELECT ",ws," FROM ", t1 , sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
wsValue  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)
RPostgres::dbDisconnect(conn)
return(wsValue)
$BODY$
  LANGUAGE plr;






CREATE OR REPLACE FUNCTION public.r_ws_values(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_ws_values('48061','10','TMAX','4')
# Just in case DROP FUNCTION r_ws_values(text,text,text,text) CASCADE 

geoid  <- as.character(arg1)
span   <- as.character(arg2)
type   <- as.character(arg3)
type   <- tolower(type)
wsNum  <- as.integer(arg4)

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )
driver <- "PostgreSQL"
drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

q0     <- base::paste("SELECT r_table_prefix('",geoid,"')",sep="")
prefix <- base::as.character( pg.spi.exec( sprintf( "%1$s", q0 ) ) )

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_",type,sep="")

q1 <- base::paste("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", t1, "' ) as g", sep="")
wsName <- base::data.frame(pg.spi.exec( sprintf( "%1$s", q1 ) ))
wsName <- wsName[2:length(wsName[,1]),]
ws <- base::paste("\"",wsName[wsNum],"\"",sep="")
q2     <- base::paste("SELECT ",ws," FROM ", t1 , sep="")
res    <- RPostgres::dbSendQuery(conn, q2)
wsValue  <- data.frame(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)
RPostgres::dbDisconnect(conn)
return(wsValue)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_check_libpaths()
  RETURNS text AS
$BODY$ 
return(.libPaths())
$BODY$
  LANGUAGE plr;

```
