-- ============================================================
-- pg.spi.foo.sql
-- Adapted from functions.sql to use pg.spi.exec instead of
-- RPostgres + YAML config. All functions run inside the Docker
-- container where PL/R is already connected to PostgreSQL.
-- No external DB connection or credentials needed.
-- ============================================================


DO $$ BEGIN
  CREATE TYPE all_coor_ws_type AS (id text, lon text, lat text);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE r_voronoi_type AS (id integer, polygon geometry);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ------------------------------------------------------------
-- load_county_tracts(county_fips)
-- Downloads Census TIGER tracts for a state, creates a
-- county-level table, and drops the statewide staging table.
-- i.e. SELECT load_county_tracts('48061')  -- Cameron County TX
-- i.e. SELECT load_county_tracts('26125')  -- Oakland County MI
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION load_county_tracts(county_fips TEXT)
RETURNS TEXT AS $$

  # Parse FIPS
  state_fips  <- substr(county_fips, 1, 2)
  county_code <- substr(county_fips, 3, 5)

  # Paths
  url         <- paste0("https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_", state_fips, "_tract.zip")
  zip_path    <- paste0("/tmp/tracts_", state_fips, ".zip")
  dir_path    <- paste0("/tmp/tracts_", state_fips)
  shp_file    <- paste0("tl_2023_", state_fips, "_tract.shp")
  shp_path    <- file.path(dir_path, shp_file)
  staging_tbl <- paste0("staging_tracts_", state_fips)
  county_tbl  <- paste0("county_tracts_", county_fips)

  # Download and unzip using R base utils
  utils::download.file(url, zip_path, method = "wget", quiet = TRUE)
  utils::unzip(zip_path, exdir = dir_path)

  # Load statewide shapefile into staging table via ogr2ogr
  base::system2("ogr2ogr", args = c(
    "-f", "PostgreSQL",
    "PG:host=/var/run/postgresql user=postgres dbname=us_gis",
    shp_path,
    "-nln", staging_tbl,
    "-nlt", "MULTIPOLYGON",
    "-overwrite"
  ))

  # Create county table and drop staging
  pg.spi.exec(paste0("DROP TABLE IF EXISTS ", county_tbl))
  pg.spi.exec(paste0("
    CREATE TABLE ", county_tbl, " AS
    SELECT * FROM ", staging_tbl, "
    WHERE statefp = '", state_fips, "'
      AND countyfp = '", county_code, "'
  "))
  pg.spi.exec(paste0("DROP TABLE ", staging_tbl))

  # Cleanup tmp files
  unlink(zip_path)
  unlink(dir_path, recursive = TRUE)

  return(paste0("Table '", county_tbl, "' created successfully."))

$$ LANGUAGE plr;


-- ------------------------------------------------------------
-- r_all_coor_ws(ghcnd, geoid, type)
-- Returns weather station coordinates for a county.
-- No RPostgres dependency — delegates to ws2pgdb package.
-- i.e. SELECT r_all_coor_ws('GHCND', '48061', 'TMAX')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_all_coor_ws(text, text, text)
  RETURNS SETOF all_coor_ws_type AS
$BODY$
  ghcnd <- arg1
  geoid <- arg2
  type  <- arg3
  ws <- ws2pgdb::all_coor_ws(ghcnd, geoid, type)
  return(ws[,c("id","longitude","latitude")])
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_county_centroid(geoid, pos)
-- Returns lat or lon centroid coordinates for a county.
-- i.e. SELECT r_county_centroid('48061', 'lat')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_county_centroid(text, text)
  RETURNS SETOF double precision AS
$BODY$

  geoid <- arg1
  pos   <- arg2

  if (pos == 'lat') {
    q1    <- base::paste("SELECT ST_Y( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lat FROM ( SELECT ST_Dump( ST_CollectionHomogenize( geom ) ) as geom FROM cb_2013_us_county_20m WHERE geoid = '", geoid, "') AS g", sep = "")
    coord <- pg.spi.exec(q1)
  }
  if (pos == 'lon') {
    q1    <- base::paste("SELECT ST_X( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lon FROM ( SELECT ST_Dump( ST_CollectionHomogenize( geom ) ) as geom FROM cb_2013_us_county_20m WHERE geoid = '", geoid, "') AS g", sep = "")
    coord <- pg.spi.exec(q1)
  }
  return(coord)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_canopi_voronoi(geoid, tableName)
-- Builds Voronoi tessellation for a county.
-- Delegates to ws2pgdb package — no RPostgres dependency.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_canopi_voronoi(text, text)
  RETURNS text AS
$BODY$
  geoid     <- arg1
  tableName <- arg2
  return(ws2pgdb::canopiVoronoi(tableName, geoid))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_county_info(geoid, type, span)
-- Orchestrates county data pipeline: tracts, MIDAS, weather.
-- i.e. SELECT r_create_county_info('12087', 'TMAX', '10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_county_info(text, text, text)
  RETURNS text AS
$BODY$

  geoid <- arg1
  type  <- arg2
  span  <- arg3
  ghcnd <- 'GHCND'

  stations    <- ws2pgdb::all_coor_ws(ghcnd, geoid, type)
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)

  tigertableName <- base::as.character(pg.spi.exec(sprintf("SELECT r_create_tiger_tracts_table('%1$s')", geoid)))

  if (identical(all.equal(tigertableName, ""), TRUE)) {
    return(tigertableName)
  }

  table_cluster <- base::paste(tigertableName, "_clustered_by_nearest_ws", sep = "")

  midastableName <- base::as.character(pg.spi.exec(sprintf("SELECT r_create_midas_synth_hh_table('%1$s')", geoid)))

  temp     <- tigertableName
  midas_pop <- base::gsub("tiger_tracts", "midas_pop", temp)
  midas_pop_clustered_by_nearest_ws <- base::paste(midas_pop, "_clustered_by_nearest_ws", sep = "")

  midasExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", table_cluster)))

  q1 <- base::paste("\
    with\
      tractce As(\
        SELECT tract.geoid10 as geoid, tract.the_geom as geom, ST_MakeLine(ST_Centroid( tract.the_geom ), coord.geom ) as geomLine, ST_Length( ST_MakeLine(ST_Centroid( tract.the_geom ), coord.geom ) ) as dist, coord.name, ST_Centroid( tract.the_geom ) as centroid FROM ", tigertableName, " tract, ", ws_metadata, " coord\
      ), \
      geoidDistance As(\
        SELECT geoid, min( dist ) as min_dist FROM tractce GROUP BY geoid\
      ),\
      line2Hub As(\
        SELECT tractce.geoid, tractce.geom as poly, ST_AsText(tractce.geomLine), tractce.centroid, name, min_dist FROM tractce, geoidDistance WHERE geoidDistance.min_dist = dist ORDER BY tractce.name\
      ),\
      cluster_ws As(\
        SELECT name, ST_MULTI( ST_UNION(poly) ) FROM line2Hub GROUP BY name\
      )\
      SELECT * INTO ", table_cluster, " FROM cluster_ws", sep = "")

  if (midasExist) {
    print("Exists!")
  } else {
    pg.spi.exec(q1)
  }

  midasClusterExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", midas_pop_clustered_by_nearest_ws)))

  q2 <- base::paste("
    with\
      tractce As(\
        SELECT ogc_fid as ws_id, tract.geoid10 as geoid, tract.the_geom as geom, ST_MakeLine(ST_Centroid( tract.the_geom), coord.geom ) as geomLine, ST_Length( ST_MakeLine(ST_Centroid( tract.the_geom), coord.geom ) ) as dist, coord.name, ST_Centroid( tract.the_geom ) as centroid FROM ", tigertableName, " tract, ", ws_metadata, " coord ORDER BY dist ASC\
      ),\
      geoidDistance As(\
        SELECT geoid, min( dist ) as dist FROM tractce GROUP BY geoid\
      ),\
      line2Hub As(\
        SELECT DISTINCT ON (gd.geoid) ws_id, t.geomLine, t.geom as poly, t.name, gd.geoid, gd.dist FROM geoidDistance gd, tractce t WHERE gd.geoid = t.geoid AND abs(gd.dist - t.dist) < 0.0000001\
      ),\
      ind_per_bg As(\
        SELECT stcotrbg As geoid, SUM(hh_size) As hh FROM ", midastableName, " GROUP BY stcotrbg ORDER BY stcotrbg\
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
      SELECT * INTO ", midas_pop_clustered_by_nearest_ws, " FROM cluster_pop ORDER BY ws_id", sep = "")

  if (midasClusterExist) {
    print("Exists!")
  } else {
    pg.spi.exec(q2)
  }

  ws2pgdb::ws_data_avg_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  ws2pgdb::ws_data_na_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)

  return(midas_pop_clustered_by_nearest_ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_midas_synth_hh_table(geoid)
-- Downloads and loads MIDAS synthetic household data.
-- i.e. SELECT r_create_midas_synth_hh_table('12087')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_midas_synth_hh_table(text)
  RETURNS text AS
$BODY$

  geoid <- arg1

  pre       <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  tableName <- paste(pre, "midas_synth_hh", sep = "")

  tableExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", tableName)))

  if (tableExist) {
    return(tableName)
  } else {
    pg.spi.exec(sprintf("SELECT r_create_synth_hh_table_template('%1$s')", tableName))

    url           <- "http://www.epimodels.org/10_Midas_Docs/SynthPop/2010/counties/"
    file          <- base::paste("2010_ver1_", geoid, sep = "")
    pfile         <- base::paste("/tmp/2010_ver1_", geoid, sep = "")
    zipFile       <- base::paste(pfile, ".zip", sep = "")
    download      <- base::paste(url, file, ".zip", sep = "")

    err <- try(utils::download.file(download, zipFile, method = "wget", quiet = TRUE))
    if (class(err) == "try-error") {
      return("")
    }

    extractedFile <- base::paste(file, "_synth_households.txt", sep = "")
    utils::unzip(zipFile, files = extractedFile, exdir = "/tmp")

    input       <- utils::read.csv(file = base::paste("/tmp/", extractedFile, sep = ""), head = TRUE, sep = ",")
    out         <- input[c("stcotrbg", "hh_race", "hh_income", "hh_size", "hh_age", "longitude", "latitude")]
    updatedFile <- base::paste(pfile, "_synth_hh.csv", sep = "")
    utils::write.csv(out, file = updatedFile, row.names = FALSE)

    pg.spi.exec(sprintf("COPY \"%1$s\" FROM '%2$s' DELIMITER ',' CSV HEADER;", tableName, updatedFile))

    unlink(zipFile)
    unlink(updatedFile)
    unlink(base::paste("/tmp/", extractedFile, sep = ""))

    return(tableName)
  }

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_synth_hh_table_template(tableName) — plpgsql, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_synth_hh_table_template(arg1 text)
  RETURNS text AS
$BODY$
DECLARE
c INT;
BEGIN
  EXECUTE 'CREATE TABLE "'
    || arg1
    || '" (stcotrbg CHAR(14), hh_race SMALLINT, hh_income DOUBLE PRECISION, hh_size SMALLINT, hh_age SMALLINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);';
  RETURN arg1;
END;
$BODY$
  LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- r_create_table(var) — plpgsql, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_table(var text)
  RETURNS integer AS
$BODY$
DECLARE
c INT;
BEGIN
  EXECUTE 'CREATE TABLE "'
    || var
    || '" (stcotrbg CHAR(14), hh_race SMALLINT, hh_income DOUBLE PRECISION, hh_size SMALLINT, hh_age SMALLINT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);';
  RETURN 0;
  EXCEPTION
    WHEN SQLSTATE '42P07' THEN RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- r_create_tiger_tracts_table(geoid)
-- Downloads Census 2023 TIGER tracts and loads into DB.
-- Modernized: HTTPS 2023 + ogr2ogr replacing FTP 2010 + shp2pgsql.
-- i.e. SELECT r_create_tiger_tracts_table('48061')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_tiger_tracts_table(text)
  RETURNS text AS
$BODY$

  geoid  <- arg1

  prefix       <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  pretableName <- base::paste(prefix, "tiger_tracts", sep = "")

  tableExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", pretableName)))

  if (tableExist) {
    return(pretableName)
  } else {
    state_fips <- substr(geoid, 1, 2)
    county_fp  <- substr(geoid, 3, 5)

    url      <- paste0("https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_", state_fips, "_tract.zip")
    zip_path <- paste0("/tmp/tracts_", state_fips, ".zip")
    dir_path <- paste0("/tmp/tracts_", state_fips)
    shp_path <- file.path(dir_path, paste0("tl_2023_", state_fips, "_tract.shp"))
    stg_tbl  <- paste0("staging_tracts_", state_fips)

    err <- try(utils::download.file(url, zip_path, method = "wget", quiet = TRUE))
    if (class(err) == "try-error") {
      return("")
    }

    utils::unzip(zip_path, exdir = dir_path)

    base::system2("ogr2ogr", args = c(
      "-f", "PostgreSQL",
      "PG:host=/var/run/postgresql user=postgres dbname=us_gis",
      shp_path,
      "-nln", stg_tbl,
      "-nlt", "MULTIPOLYGON",
      "-overwrite"
    ))

    pg.spi.exec(paste0("
      CREATE TABLE \"", pretableName, "\" AS
      SELECT * FROM ", stg_tbl, "
      WHERE statefp = '", state_fips, "' AND countyfp = '", county_fp, "'
    "))

    pg.spi.exec(paste0("DROP TABLE ", stg_tbl))

    unlink(zip_path)
    unlink(dir_path, recursive = TRUE)

    return(pretableName)
  }

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_avg_span_2_pgdb — no RPostgres, unchanged
-- i.e. SELECT r_gen_data_avg_span_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_avg_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_data  <- ws2pgdb::ws_data_avg_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_data)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_na_span_2_pgdb — no RPostgres, unchanged
-- i.e. SELECT r_gen_data_na_span_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_na_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_data  <- ws2pgdb::ws_data_na_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_data)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_span_avg_2_pgdb — no RPostgres, unchanged
-- i.e. SELECT r_gen_data_span_avg_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_span_avg_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  stations <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_metadata <- ws2pgdb::ws_data_span_avg_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_metadata)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_span_na_2_pgdb — no RPostgres, unchanged
-- i.e. SELECT r_gen_data_span_na_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_span_na_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  stations <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_metadata <- ws2pgdb::ws_data_span_na_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_metadata)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_population_size(geoid)
-- Returns total synthetic population size for a county.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_population_size(text)
  RETURNS integer AS
$BODY$

  geoid      <- arg1
  nameTable  <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))
  midasTable <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws", sep = "")
  exists     <- as.integer(pg.spi.exec(base::paste("SELECT r_table_exists('", midasTable, "')", sep = "")))

  if (exists) {
    total <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh) FROM ", midasTable, sep = "")))
  } else {
    midasTable <- base::paste(nameTable, "midas_synth_hh", sep = "")
    total      <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep = "")))
  }
  return(total)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_pos(geoid, pos)
-- Returns lat or lon for weather stations in a county.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_pos(text, text)
  RETURNS SETOF double precision AS
$BODY$

  geoid <- arg1
  pos   <- arg2
  type  <- 'TMAX'
  span  <- '3'

  if (pos == 'lat') {
    tableName <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))
    type      <- tolower(type)
    tableName <- paste(tableName, "ws_metadata_span_", span, "_", type, sep = "")
    coord     <- data.frame(pg.spi.exec(base::paste("SELECT ST_Y(lat.geom) FROM ", tableName, " as lat", sep = "")))
  }
  if (pos == 'lon') {
    tableName <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))
    type      <- tolower(type)
    tableName <- paste(tableName, "ws_metadata_span_", span, "_", type, sep = "")
    coord     <- data.frame(pg.spi.exec(base::paste("SELECT ST_X(lon.geom) FROM ", tableName, " as lon", sep = "")))
  }
  return(coord)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_r_version() — no RPostgres, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_r_version()
  RETURNS text AS
$BODY$
  return(getRversion())
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_tract_size(geoid, wsNum)
-- Returns household count for a given weather station index.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_tract_size(text, text)
  RETURNS integer AS
$BODY$

  geoid <- as.character(arg1)
  wsNum <- as.integer(arg2)

  if (wsNum == 0) {
    return(0)
  }

  nameTable  <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))
  midasTable <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws ", sep = "")
  exists     <- as.integer(pg.spi.exec(base::paste("SELECT r_table_exists('", midasTable, "')", sep = "")))

  if (exists) {
    wsNum  <- wsNum - 1
    str1   <- base::paste("SELECT ws_id FROM ", midasTable, " LIMIT 1 OFFSET ", wsNum, sep = "")
    rowNum <- data.frame(pg.spi.exec(str1))
    q3     <- base::paste("SELECT hh FROM ", midasTable, " WHERE ws_id = ", rowNum, sep = "")
    total  <- data.frame(pg.spi.exec(q3))
  } else {
    total <- 0
  }
  return(total)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_model_colname(geoid, span, disease, wsNum)
-- Returns model column name for a given weather station.
-- i.e. SELECT r_model_colname('12087','10','dengue','4')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_model_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid   <- as.character(arg1)
  span    <- as.character(arg2)
  disease <- arg3
  wsNum   <- as.integer(arg4)

  prefix  <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_tmax_", disease, sep = "")

  wsModel <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsModel <- wsModel[1:length(wsModel[,1]),]
  ws      <- base::paste("\"", wsModel[wsNum], "\"", sep = "")
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_model_values(geoid, span, disease, wsNum, type)
-- Returns model values for a given weather station.
-- i.e. SELECT r_model_values('12087','10','dengue','4','TMAX')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_model_values(text, text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid   <- base::as.character(arg1)
  span    <- base::as.character(arg2)
  disease <- base::as.character(arg3)
  wsNum   <- base::as.integer(arg4)
  type    <- base::tolower(base::as.character(arg5))

  prefix  <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_", type, "_", disease, sep = "")

  wsModel <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsModel <- wsModel[1:length(wsModel[,1]),]
  ws      <- base::paste("\"", wsModel[wsNum], "\"", sep = "")
  wsValue <- base::data.frame(pg.spi.exec(sprintf("SELECT %1$s FROM %2$s", ws, t1)))
  return(wsValue)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_name_2_fips(name, key)
-- Converts a state or county name to FIPS code.
-- i.e. SELECT r_name_2_fips('Monroe', 'co')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_name_2_fips(text, text)
  RETURNS text AS
$BODY$

  name <- arg1
  key  <- arg2

  if (key == 'st') {
    q1  <- base::paste("SELECT GEOID FROM cb_2013_us_state_20m WHERE NAME = '", name, "'", sep = "")
    nom <- pg.spi.exec(q1)
  }
  if (key == 'co') {
    q1  <- base::paste("SELECT GEOID FROM cb_2013_us_county_20m WHERE NAME = '", name, "'", sep = "")
    nom <- pg.spi.exec(q1)
  }
  return(nom)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_fips_2_state(geoid)
-- Converts a state or county FIPS to state name.
-- i.e. SELECT r_fips_2_state('12') or r_fips_2_state('12087')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_fips_2_state(text)
  RETURNS text AS
$BODY$

  geoid <- arg1
  nom   <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", substr(geoid, 1, 2)))
  return(nom)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_read_population_size(geoid)
-- Reads total population from MIDAS tables.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_read_population_size(text)
  RETURNS integer AS
$BODY$

  geoid     <- arg1
  nameTable <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))

  midasTable <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws", sep = "")
  exists     <- as.integer(pg.spi.exec(base::paste("SELECT r_table_exists('", midasTable, "')", sep = "")))

  if (exists) {
    total <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh) FROM ", midasTable, sep = "")))
  } else {
    midasTable <- base::paste(nameTable, "midas_synth_hh", sep = "")
    total      <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep = "")))
  }
  return(total)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_read_ws(geoid, type, span)
-- Returns weather station names for a county.
-- i.e. SELECT r_read_ws('12087', 'TMAX', '10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_read_ws(text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid <- arg1
  type  <- arg2
  span  <- arg3

  nameTable     <- data.frame(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  metainfoTable <- base::paste(nameTable, "ws_metadata_span_", span, "_", type, sep = "")
  ws            <- data.frame(pg.spi.exec(sprintf("SELECT name FROM %1$s", metainfoTable)))
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_table_exists(var) — plpgsql, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_table_exists(var text)
  RETURNS integer AS
$BODY$
DECLARE
c INT;
BEGIN
  EXECUTE 'SELECT * FROM ' || var || ';';
  RETURN 1;
  EXCEPTION
    WHEN SQLSTATE '42P01' THEN RETURN 0;
END;
$BODY$
  LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- r_table_prefix(geoid)
-- Generates a table name prefix from a FIPS code.
-- i.e. SELECT r_table_prefix('12087')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_table_prefix(text)
  RETURNS text AS
$BODY$

  geoid <- arg1

  if (as.integer(geoid) < 100) {
    state     <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", geoid))
    tableName <- base::paste(state, "_", geoid, "_", sep = "")
  } else {
    county    <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_county_20m WHERE GEOID='%1$s'", geoid))
    state     <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", substr(geoid, 1, 2)))
    tableName <- base::paste(state, "_", county, "_", geoid, "_", sep = "")
  }

  varTable  <- tolower(tableName)
  tableName <- gsub(" ", "_", varTable)
  tableName <- gsub("-", "_", varTable)
  return(tableName)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_version() — no RPostgres, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_version()
  RETURNS SETOF r_version_type AS
$BODY$
  cbind(names(version),unlist(version))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_voronoi(tableName, geomCol, idCol) — already uses pg.spi.exec, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_voronoi(text, text, text)
  RETURNS SETOF r_voronoi_type AS
$BODY$
  library(deldir)
  points <- pg.spi.exec(
    sprintf("SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;", arg1, arg2)
  )
  buffer_distance = ((abs(max(points$x) - min(points$x)) + abs(max(points$y) - min(points$y))) / 2) * (0.50)
  buffer_set <- pg.spi.exec(
    sprintf("SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;", arg1, arg2, buffer_distance)
  )
  voro = deldir(points$x, points$y, digits=22, frac=0.00000000000000000000000001, list(ndx=2,ndy=2),
    rw=c(min(points$x) - abs(min(points$x) - max(points$x)), max(points$x) + abs(min(points$x) - max(points$x)),
         min(points$y) - abs(min(points$y) - max(points$y)), max(points$y) + abs(min(points$y) - max(points$y))))
  tiles = tile.list(voro)
  poly = array()
  id = array()
  p = 1
  for (i in 1:length(tiles)) {
    tile = tiles[[i]]
    curpoly = "POLYGON(("
    for (j in 1:length(tile$x)) {
      curpoly = sprintf("%s %.6f %.6f,", curpoly, tile$x[[j]], tile$y[[j]])
    }
    curpoly = sprintf("%s %.6f %.6f))", curpoly, tile$x[[1]], tile$y[[1]])
    ipoint <- pg.spi.exec(
      sprintf("SELECT %3$s AS id, st_intersection('SRID='||st_srid(%2$s)||';%4$s'::text,'%5$s') AS polygon FROM %1$s WHERE st_intersects(%2$s::text,'SRID='||st_srid(%2$s)||';%4$s');",
              arg1, arg2, arg3, curpoly, buffer_set$ewkb[1])
    )
    if (length(ipoint) > 0) {
      poly[[p]] <- ipoint$polygon[1]
      id[[p]]   <- ipoint$id[1]
      p = (p + 1)
    }
  }
  return(data.frame(id, poly))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_voronoi_scale(tableName, geomCol, idCol, scale) — already uses pg.spi.exec, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_voronoi_scale(text, text, text, double precision)
  RETURNS SETOF r_voronoi_type AS
$BODY$
  library(deldir)
  points <- pg.spi.exec(
    sprintf("SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;", arg1, arg2)
  )
  buffer_distance = ((abs(max(points$x) - min(points$x)) + abs(max(points$y) - min(points$y))) / 2) * (arg4)
  buffer_set <- pg.spi.exec(
    sprintf("SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;", arg1, arg2, buffer_distance)
  )
  voro = deldir(points$x, points$y, digits=22, frac=0.00000000000000000000000001, list(ndx=2,ndy=2),
    rw=c(min(points$x) - abs(min(points$x) - max(points$x)), max(points$x) + abs(min(points$x) - max(points$x)),
         min(points$y) - abs(min(points$y) - max(points$y)), max(points$y) + abs(min(points$y) - max(points$y))))
  tiles = tile.list(voro)
  poly = array()
  id = array()
  p = 1
  for (i in 1:length(tiles)) {
    tile = tiles[[i]]
    curpoly = "POLYGON(("
    for (j in 1:length(tile$x)) {
      curpoly = sprintf("%s %.6f %.6f,", curpoly, tile$x[[j]], tile$y[[j]])
    }
    curpoly = sprintf("%s %.6f %.6f))", curpoly, tile$x[[1]], tile$y[[1]])
    ipoint <- pg.spi.exec(
      sprintf("SELECT %3$s AS id, st_intersection('SRID='||st_srid(%2$s)||';%4$s'::text,'%5$s') AS polygon FROM %1$s WHERE st_intersects(%2$s::text,'SRID='||st_srid(%2$s)||';%4$s');",
              arg1, arg2, arg3, curpoly, buffer_set$ewkb[1])
    )
    if (length(ipoint) > 0) {
      poly[[p]] <- ipoint$polygon[1]
      id[[p]]   <- ipoint$id[1]
      p = (p + 1)
    }
  }
  return(data.frame(id, poly))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_ws_colname(geoid, span, disease, wsNum)
-- Returns weather station column name for a given index.
-- i.e. SELECT r_ws_colname('12087','10','dengue','4')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_ws_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid   <- as.character(arg1)
  span    <- as.character(arg2)
  disease <- arg3
  wsNum   <- as.integer(arg4)

  prefix  <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_tmax", sep = "")

  wsName  <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsName  <- wsName[2:length(wsName[,1]),]
  ws      <- base::paste("\"", wsName[wsNum], "\"", sep = "")
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_ws_values(geoid, span, type, wsNum)
-- Returns weather data values for a given station index.
-- i.e. SELECT r_ws_values('12087','10','TMAX','4')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_ws_values(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid  <- as.character(arg1)
  span   <- as.character(arg2)
  type   <- tolower(as.character(arg3))
  wsNum  <- as.integer(arg4)

  prefix <- as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1     <- base::paste(prefix, "ws_data_span_", span, "_avg_", type, sep = "")

  wsName <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsName <- wsName[2:length(wsName[,1]),]
  ws     <- base::paste("\"", wsName[wsNum], "\"", sep = "")
  wsValue <- data.frame(pg.spi.exec(sprintf("SELECT %1$s FROM %2$s", ws, t1)))
  return(wsValue)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_check_libpaths() — no RPostgres, unchanged
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_check_libpaths()
  RETURNS text AS
$BODY$
  return(.libPaths())
$BODY$
  LANGUAGE plr;
