# ws2pgdb
Package contains several functions that retrieve, filter, and store NOAA data in a local/remote Postgres database. 
One useful function is that that takes weather station location in a region and constructs a Voronoi tessellation over that region (you will need a GIS software to visualize the output which is a set of geometries/polygones). 
Another useful function within the package is that that iteratively request several years of data from the NOAA digital warehouse. Since NOAA permits only one year of data requested at a time, via the rnoaa package. It is particular useful when many years of information are needed. 

Problems:

Some system libraries are needed before you can start using the ws2pgdb. Some of the methods necesitate the octave development tools, gdal, proj (octave-dev, liboctave, libgdal, libproj-dev). Try to install the ws2pgdb and the installer will let you know when a library is needed. This maybe a slow process but if you are using any debian-like, you can make used of the package manager to install them (apt-get). 


During a fresh installation, after a couple of months without touching this libraries, I tried devtools::install_github(rabravo/ws2pgdb) and it complained that some of the dependencies were not compatible. To install these dependencies you shall do as in the following example:

  devtools::install_github("rstats-db/RPostgres")

or

  sudo su - -c "R -e \"install.packages('packagename', repos='http://cran.rstudio.com/')\""

CENSUS DATA

Function in this bundle request information from your local database. The database is assumed to contain U.S. Census data. The data is the cartographic boundary files a.k.a TIGER files in SHAPEFILE format. In the past I used QGIS to upload this files into my Geographic-enable Postgres database but these shapefiles can very well be uploaded via command line using psql and shp2psql script. 

The data is available in the following website. 
https://www.census.gov/geo/maps-data/data/cbf/cbf_counties.html

For my dissertation, the TIGER files from the year 2013 were used: nation, state, county. In the future, I will add a variable so that the functions in the libraries can make use of any available year. Since the convention for file naming varies only the year of file for the corresponding new data set.

#PLR/SQL and PLPGSQL
The following function enables the communication between the vector-borne simulator and the pgsql ( database ).

Copy, paste, and execute these queries on the pgsql server.

```

CREATE TYPE all_coor_ws_type AS (id text ,lon text ,lat text );

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


--This function below here is not properly working; fix this or remove soon.

CREATE OR REPLACE FUNCTION public.r_column_names(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

#THIS FUNCTINO IS NOT WORKING
#i.e. SELECT r_column_names('48061','10','malaria')

geoid  <- as.character(arg1)
span   <- as.character(arg2)
disease<- arg3
num    <- as.integer(arg4)

file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
config <- yaml::yaml.load_file( file )

drv    <- RPostgres::Postgres()
conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


res    <- RPostgres::dbSendQuery( conn, sprintf("SELECT r_table_prefix('%1$s')", geoid) )
prefix <- base::as.character(RPostgres::dbFetch(res))
RPostgres::dbClearResult(res)

t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax",sep="")
t2  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax_",disease,sep="")

res    <- RPostgres::dbSendQuery( conn, sprintf( "SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1 ) )
wsName <- base::data.frame(RPostgres::dbFetch(res))
wsName <- wsName[2:length(wsName[,1]),]
RPostgres::dbClearResult(res)

res    <- RPostgres::dbSendQuery( conn, sprintf( "SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t2 ) )
wsModel <- base::data.frame(RPostgres::dbFetch(res))
wsTempAndModel <- data.frame( c( wsName[num], wsModel[num] ) )
RPostgres::dbClearResult(res)

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

  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)


  stations       <-  ws2pgdb::all_coor_ws( ghcnd, geoid, type) 
  ws_metadata    <- ws2pgdb::ws_metadata_span_2_pgdb( geoid, type, stations, span ) 

  res            <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_create_tiger_tracts_table('%1$s')", geoid))
  tigertableName <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  if( identical(all.equal(tigertableName, ""), TRUE) ) {
    RPostgres::dbDisconnect(conn)
    return(tigertableName)
  }


  table_cluster  <- base::paste( tigertableName, "_clustered_by_nearest_ws", sep="")

  # tractce) Build the distance matrix from the subregion's centroid to all weather stations
  # geoid: tract geoid; path2Hub: text form of geom; 
  # geom As poly: tract polygone
  # geom: line between the a) a tract centroid, b) a weather station;
  # dist: length of the geometry
  # name: name of weather station

  res            <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_create_midas_synth_hh_table('%1$s')", geoid))
  midastableName <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  temp           <- tigertableName
  midas_pop      <- base::gsub("tiger_tracts", "midas_pop", temp)
  midas_pop_clustered_by_nearest_ws <- base::paste(midas_pop,"_clustered_by_nearest_ws",sep="")

  res            <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_exists('%1$s')", table_cluster))
  midasExist     <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  q1 <- base::paste("\
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
      SELECT * INTO ", table_cluster, " FROM cluster_ws ",  sep="")

  if ( midasExist ) {
    print("Exists!");
  } else {
    res        <- RPostgres::dbSendQuery(conn, q1)
    q1_out     <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
  }

  res          <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_exists('%1$s')", midas_pop_clustered_by_nearest_ws))
  midasClusterExist <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

#----------Prepared Query / May be executed or not --------------------------

    q2 <- base::paste("
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

  if ( midasClusterExist ) {
  
    print("Exists!");
    
  } else {
  
    res      <- RPostgres::dbSendQuery(conn, q2)
    q2_out   <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)

  }

  ws2pgdb::ws_data_avg_span_2_pgdb( ghcnd, geoid, type, span, ws_metadata )
  ws2pgdb::ws_data_na_span_2_pgdb(  ghcnd, geoid, type, span, ws_metadata )

  RPostgres::dbDisconnect(conn)  
  return(midas_pop_clustered_by_nearest_ws)

$BODY$
  LANGUAGE plr;


CREATE OR REPLACE FUNCTION public.r_create_midas_synth_hh_table(text)
  RETURNS text AS
$BODY$
#i.e. SELECT r_create_midas_synth_hh_table('12087')

  pwd     <- base::Sys.getenv("PWD")
  geoid       <- arg1

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )

  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  res    <- RPostgres::dbSendQuery( conn, sprintf("SELECT r_table_prefix('%1$s')", geoid) )
  pre  <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  tableName  <- paste(pre,"midas_synth_hh", sep="")
  res        <- RPostgres::dbSendQuery( conn, sprintf("SELECT r_table_exists('%1$s')", tableName) )
  tableExist <- as.integer(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  if ( tableExist ) {

    RPostgres::dbDisconnect(conn)
    return(tableName)

  } else {

    res   <- RPostgres::dbSendQuery(conn, sprintf( "SELECT r_create_synth_hh_table_template('%1$s')", tableName ) )
    debug <- as.integer(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    
    url     <- "http://www.epimodels.org/10_Midas_Docs/SynthPop/2010/counties/"
    file      <- base::paste("2010_ver1_",geoid,sep="")
    pfile     <- base::paste(pwd,"/","2010_ver1_",geoid,sep="")
    extractedFile <- base::paste(file, "_synth_households.txt", sep="")
    updatedFile   <- base::paste(pfile, "_synth_hh.csv", sep="")
    zipFile   <- base::paste(pfile,".zip", sep="")
    download  <- base::paste(url, file, ".zip",sep="")
    err <- try (curl::curl_download( download, zipFile, quiet = TRUE ) )
    if (class(err) == "try-error") {
      RPostgres::dbClearResult(res)
      RPostgres::dbDisconnect(conn)
      return("")
    }
    utils::unzip(paste(pfile,".zip",sep=""), file = extractedFile) 
    input     <- utils::read.csv( file = extractedFile, head = TRUE, sep=",")
    out       <- input[c("stcotrbg", "hh_race", "hh_income","hh_size", "hh_age","longitude","latitude")]
    utils::write.csv(out, file = updatedFile, row.names = FALSE)

    res   <- RPostgres::dbSendQuery(conn, sprintf( "COPY \"%1$s\" FROM '%2$s' DELIMITER ',' CSV HEADER;", tableName, updatedFile) )
    err <- as.character(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    
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

  pgfile <- base::paste(Sys.getenv("PWD"), "/","pg_config.yml", sep="")
  print(pgfile)
  pwd    <- base::Sys.getenv("PWD")
  print(pwd)
  config <- yaml::yaml.load_file( pgfile )
  geoid  <- arg1

  # To override auth, provide your passwd via the .pgpass (see postgresql documentation)
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_prefix('%1$s')", geoid))
  prefix <- base::as.character(RPostgres::dbFetch(res))
  pretableName <- base::paste(prefix, "tiger_tracts",sep="")
  RPostgres::dbClearResult(res)

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_exists('%1$s')", pretableName) )
  tableExist <- base::as.integer( RPostgres::dbFetch(res) )
  RPostgres::dbClearResult(res)

  if ( tableExist ) {
    return(pretableName)
  } else {

    res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_fips_2_state('%1$s')", geoid) )
    state  <- base::as.character(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    state  <- gsub(" ", "_", state)
    ftp    <- "ftp://ftp2.census.gov/geo/pvs/tiger2010st/"
    url    <- base::paste(ftp, substr(geoid, 1, 2), "_", state, "/", geoid, "/", sep="")
    file   <- base::paste("tl_2010_",geoid,"_tract10",sep="")
    tableName  <- base::paste(file, sep="")
    ext    <- ".zip"
    pfile  <- base::paste(pwd,"/",file,sep="")
    download   <- base::paste(url, file, ext, sep="")
    zipFile    <- base::paste(pwd,"/temp/",file, ext, sep="")
    err <- try( curl::curl_download(download, zipFile, quiet = TRUE ) )
    if (class(err) == "try-error") {
      RPostgres::dbClearResult(res)
      RPostgres::dbDisconnect(conn)
      return("")
    }
    dir    <- base::paste(pwd,"/","temp",sep="")
    utils::unzip(zipFile, overwrite = 'TRUE', exdir=dir)
    scriptFile    <- base::paste(file, ".sql", sep="")
    sqlscriptPath <- base::paste(pwd, "/temp/", scriptFile, sep="")
    q5 <- base::paste("shp2pgsql -c -s 4269 -g the_geom -W latin1 ",pwd, "/", "temp/", file, ".shp", " public.", pretableName, " " ,config$dbname," > ", sqlscriptPath, sep="")
    system(q5)
    q6 <- base::paste("set PGPASSWORD=config$dbpwd & psql -d ", config$dbname, " -U ", config$dbuser, " -p ", config$dbport, " -q --file='", sqlscriptPath, "'",sep="")
    system(q6)
    q7 <- paste("rm ", pwd, "/", "temp/*", sep = "")
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

  if ( exists ) {
    q3     <- base::paste("SELECT SUM(hh) FROM ", midasTable, sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    total  <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
  } else {
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

  # geoid is the fips number. This function only accept fips for counties (5 digits)
  geoid <- arg1
  pos   <- arg2

  # Weathe variable
  type  <- 'TMAX'
  # Look-back in time
  span  <- '3'

  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if ( pos == 'lat') {
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
  if ( pos == 'lon') {
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
  if (wsNum == 0) {
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

  if ( exists ) {
    # Fail badly if table entry does not exist
    wsNum  <- wsNum - 1
    str1   <- base::paste("SELECT ws_id FROM ", midasTable," LIMIT 1 OFFSET ", wsNum , sep="")
    res    <- RPostgres::dbSendQuery(conn,str1)
    rowNum  <- data.frame(RPostgres::dbFetch(res))

    q3     <- base::paste("SELECT hh FROM ", midasTable,"WHERE ws_id = ",rowNum, sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    total  <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
  } else {
    total <- 0
  }
  
  RPostgres::dbDisconnect(conn)
  return(total)
$BODY$
  LANGUAGE plr;




CREATE OR REPLACE FUNCTION public.r_model_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
# i.e. SELECT r_model_colname('12087','10','dengue','4')

  geoid  <- as.character(arg1)
  span   <- as.character(arg2)
  disease<- arg3
  wsNum  <- as.integer(arg4)

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  res     <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_prefix('%1$s')", geoid))
  prefix  <- base::as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax_",disease,sep="")

  res      <- RPostgres::dbSendQuery(conn, sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1))
  wsModel  <- base::data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  wsModel  <- wsModel[1:length(wsModel[,1]),]
  ws       <- base::paste("\"",wsModel[wsNum],"\"",sep="")

  return(ws)

$BODY$
  LANGUAGE plr; 



CREATE OR REPLACE FUNCTION public.r_model_values(text, text, text, text, text)
  RETURNS SETOF text AS
$BODY$
  # i.e. SELECT r_model_values('12087','10','dengue','4','TMAX')
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

  res     <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_prefix('%1$s')", geoid))
  prefix  <- base::as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_",type,"_",disease,sep="")

  res     <- RPostgres::dbSendQuery(conn, sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1))
  wsModel <- base::data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  wsModel <- wsModel[1:length(wsModel[,1]),]
  ws      <- base::paste("\"",wsModel[wsNum],"\"",sep="")

  res     <- RPostgres::dbSendQuery(conn, sprintf("SELECT %1$s FROM %2$s", ws, t1) )
  wsValue  <- base::data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
  return(wsValue)
  
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_name_2_fips(text, text)
  RETURNS text AS
$BODY$ 

  # First arguments needs the Official name that is it has to include Capitalization
  # Second arguments can take the value of 'st' for state, or 'co' for county
  # Return one or more fips 
  # i.e. SELECT r_name_2_fips('Monroe', 'co')

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



CREATE OR REPLACE FUNCTION public.r_fips_2_state(text)
  RETURNS text AS
$BODY$ 

  # i.e. SELECT r_fips_2_state('12') or SELECT r_fips_2_state('12087')
  # out: 'Florida'

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  geoid   <- arg1


  driver <- "PostgreSQL"
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  res  <- RPostgres::dbSendQuery(conn, sprintf("select NAME from cb_2013_us_state_20m where GEOID='%1$s'", substr(geoid, 1, 2) ) )
  nom  <- RPostgres::dbFetch(res)
  RPostgres::dbClearResult(res)
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

  if ( exists ) {
    q3     <- base::paste("SELECT SUM(hh) FROM ", midasTable, sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    total  <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
  } else {
    midasTable    <- base::paste(nameTable, "midas_synth_hh" ,sep="")
    q3     <- base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep="")
    res    <- RPostgres::dbSendQuery(conn, q3)
    total  <- data.frame(RPostgres::dbFetch(res))
    RPostgres::dbClearResult(res)
    # return(base::paste(midasTable, " does not exists ,sep="") )
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

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_prefix('%1$s')", geoid) )
  nameTable  <- data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  metainfoTable    <- base::paste(nameTable, "ws_metadata_span_",span,"_",type ,sep="")

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT name FROM %1$s", metainfoTable) )
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

  # i.e. SELECT r_table_prefix('12087')

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  geoid  <- arg1

  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  if ( as.integer(geoid) < 100) {
    
    res   <- RPostgres::dbSendQuery(conn, sprintf("select NAME from cb_2013_us_state_20m where GEOID='%1$s'", geoid))
    state <- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res)
    
    tableName <- base::paste(state,"_",geoid,"_",sep="")

  } else {

    res   <- RPostgres::dbSendQuery(conn, sprintf("select NAME from cb_2013_us_county_20m where GEOID='%1$s'", geoid))
    county<- RPostgres::dbFetch(res)
    RPostgres::dbClearResult(res) 

    res   <- RPostgres::dbSendQuery(conn, sprintf("select NAME from cb_2013_us_state_20m where GEOID='%1$s'", substr(geoid, 1, 2)))
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


CREATE TYPE r_voronoi_type AS (id integer, polygon geometry);


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
  # i.e. SELECT r_ws_colname('12087','10','dengue','4')
  geoid  <- as.character(arg1)
  span   <- as.character(arg2)
  disease<- arg3
  wsNum    <- as.integer(arg4)

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT r_table_prefix('%1$s')", geoid) )
  prefix <- base::as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_tmax",sep="")

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1) )
  wsName <- base::data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  wsName <- wsName[2:length(wsName[,1]),]
  ws     <- base::paste("\"",wsName[wsNum],"\"",sep="")
  return(ws)
$BODY$
  LANGUAGE plr;



CREATE OR REPLACE FUNCTION public.r_ws_values(text, text, text, text)
  RETURNS SETOF text AS
$BODY$
  # i.e. SELECT r_ws_values('12087','10','TMAX','4')
  # Just in case DROP FUNCTION r_ws_values(text,text,text,text) CASCADE 

  geoid  <- as.character(arg1)
  span   <- as.character(arg2)
  type   <- as.character(arg3)
  type   <- tolower(type)
  wsNum  <- as.integer(arg4)

  file   <- base::paste(Sys.getenv("HOME"), "/","pg_config.yml", sep="")
  config <- yaml::yaml.load_file( file )
  drv    <- RPostgres::Postgres()
  conn   <- RPostgres::dbConnect(drv, host= config$dbhost, port= config$dbport, dbname= config$dbname, user= config$dbuser, password= config$dbpwd)
  res    <- RPostgres::dbSendQuery( conn, sprintf("SELECT r_table_prefix('%1$s')", geoid) )
  prefix <- as.character(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)

  t1  <- base::paste(prefix,"ws_data_span_",span,"_avg_",type,sep="")

  res    <- RPostgres::dbSendQuery( conn, sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1) )
  wsName <- base::data.frame(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res) 

  wsName <- wsName[2:length(wsName[,1]),]
  ws <- base::paste("\"",wsName[wsNum],"\"",sep="")

  res    <- RPostgres::dbSendQuery(conn, sprintf("SELECT %1$s FROM %2$s", ws, t1) )
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
