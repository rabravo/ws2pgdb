-- ============================================================
-- pg.spi.foo.sql
--
-- PL/R and PL/pgSQL functions for the us_gis database.
--
-- These functions were adapted from functions.sql to use
-- pg.spi.exec() instead of RPostgres + YAML credentials.
-- Since PL/R runs inside PostgreSQL, it shares the same
-- transaction and connection — no TCP socket, no auth,
-- no external dependencies at runtime.
--
-- Architecture overview:
--   SQL call → PL/R function → pg.spi.exec() → PostgreSQL
--                           → ws2pgdb R package (installed at image build)
--                           → external data (NOAA, Census, FRED)
--
-- Data sources:
--   NOAA GHCND  — weather station data via rnoaa (pre-Sept 2022)
--   Census TIGER — tract shapefiles (2023) via www2.census.gov
--   FRED        — synthetic population (2010 v1) via fred.publichealth.pitt.edu
--
-- Loaded automatically into us_gis on container first run
-- by docker/init-db.sh.
-- ============================================================


-- ------------------------------------------------------------
-- Custom composite types
--
-- all_coor_ws_type: returned by r_all_coor_ws() — holds a
--   weather station identifier and its coordinates.
--   lon/lat stored as text to preserve NOAA string format.
--
-- r_voronoi_type: returned by r_voronoi() and r_voronoi_scale()
--   — maps a point ID to its Voronoi polygon geometry.
--   Used by downstream CANOPI epidemic model functions.
-- ------------------------------------------------------------
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
--
-- Downloads the statewide Census 2023 TIGER tract shapefile,
-- loads it into a temporary staging table via ogr2ogr, then
-- filters to the requested county and saves it as a permanent
-- county-level table. The staging table is dropped afterward
-- to keep the schema clean.
--
-- The FIPS code is split: first 2 digits = state, last 3 = county.
-- Table naming convention: county_tracts_{fips}
-- e.g. county_tracts_48061 for Cameron County, TX
--
-- Called interactively via manage.sh option 9, or directly:
--   SELECT load_county_tracts('48061')  -- Cameron County, TX
--   SELECT load_county_tracts('26125')  -- Oakland County, MI
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION load_county_tracts(county_fips TEXT)
RETURNS TEXT AS $$

  # Split the 5-digit FIPS into state (2) and county (3) components
  state_fips  <- substr(county_fips, 1, 2)
  county_code <- substr(county_fips, 3, 5)

  # Build paths — statewide ZIP downloaded to /tmp, extracted into subfolder
  url         <- paste0("https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_", state_fips, "_tract.zip")
  zip_path    <- paste0("/tmp/tracts_", state_fips, ".zip")
  dir_path    <- paste0("/tmp/tracts_", state_fips)
  shp_file    <- paste0("tl_2023_", state_fips, "_tract.shp")
  shp_path    <- file.path(dir_path, shp_file)

  # staging_tbl holds all tracts for the state; county_tbl is the filtered result
  staging_tbl <- paste0("staging_tracts_", state_fips)
  county_tbl  <- paste0("county_tracts_", county_fips)

  # Download and extract — wget used inside container (curl also available)
  utils::download.file(url, zip_path, method = "wget", quiet = TRUE)
  utils::unzip(zip_path, exdir = dir_path)

  # ogr2ogr loads the shapefile into PostgreSQL using the Unix domain socket
  # URI (host=/var/run/postgresql) to avoid TCP auth inside the container.
  # -nlt MULTIPOLYGON ensures consistent geometry type across all tracts.
  # -overwrite allows re-running without dropping the staging table manually.
  ogr_result <- base::system2("/usr/bin/ogr2ogr", args = c(
    "-f", "PostgreSQL",
    "postgresql://postgres@/us_gis?host=/var/run/postgresql",
    shp_path,
    "-nln", staging_tbl,
    "-nlt", "MULTIPOLYGON",
    "-overwrite"
  ), stdout = TRUE, stderr = TRUE)
  if (!is.null(attr(ogr_result, "status")) && attr(ogr_result, "status") != 0) {
    stop(paste("ogr2ogr failed:", paste(ogr_result, collapse = "\n")))
  }

  # Filter statewide staging table down to just the requested county
  pg.spi.exec(paste0("DROP TABLE IF EXISTS ", county_tbl))
  pg.spi.exec(paste0("
    CREATE TABLE ", county_tbl, " AS
    SELECT * FROM ", staging_tbl, "
    WHERE statefp = '", state_fips, "'
      AND countyfp = '", county_code, "'
  "))

  # Drop staging — no longer needed, keeps schema uncluttered
  pg.spi.exec(paste0("DROP TABLE ", staging_tbl))

  # Clean up /tmp — ZIP and extracted shapefile directory
  unlink(zip_path)
  unlink(dir_path, recursive = TRUE)

  return(paste0("Table '", county_tbl, "' created successfully."))

$$ LANGUAGE plr;


-- ------------------------------------------------------------
-- r_all_coor_ws(ghcnd, geoid, type)
--
-- Thin PL/R wrapper around ws2pgdb::all_coor_ws(). Retrieves
-- all NOAA weather stations for a county that carry the
-- requested variable type (TMAX, TMIN, PRCP, etc.).
--
-- The ws2pgdb package handles NOAA API pagination and reads
-- the NOAA token from ~/pg_config.yml inside the container.
--
-- Returns a set of (id, lon, lat) rows — one per station.
-- These coordinates are used downstream to build Voronoi
-- tessellations and assign census tracts to their nearest
-- weather station.
--
-- i.e. SELECT r_all_coor_ws('GHCND', '48061', 'TMAX')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_all_coor_ws(text, text, text)
  RETURNS SETOF all_coor_ws_type AS
$BODY$
  ghcnd <- arg1   -- always 'GHCND' for daily historical data
  geoid <- arg2   -- 5-digit county FIPS
  type  <- arg3   -- weather variable: TMAX, TMIN, PRCP, etc.
  ws <- ws2pgdb::all_coor_ws(ghcnd, geoid, type)
  return(ws[,c("id","longitude","latitude")])
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_county_centroid(geoid, pos)
--
-- Extracts the latitude ('lat') or longitude ('lon') coordinates
-- of all vertices of a county boundary polygon. The county
-- geometry is read from cb_2013_us_county_20m — the 2013 Census
-- cartographic boundary file that must be preloaded into us_gis.
--
-- ST_CollectionHomogenize normalizes mixed geometry types before
-- ST_Dump breaks the polygon into individual rings, and
-- ST_DumpPoints extracts each vertex. Returns one coordinate
-- value per vertex — useful for bounding box calculations and
-- centroid approximations.
--
-- i.e. SELECT r_county_centroid('48061', 'lat')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_county_centroid(text, text)
  RETURNS SETOF double precision AS
$BODY$

  geoid <- arg1   -- 5-digit county FIPS
  pos   <- arg2   -- 'lat' or 'lon'

  if (pos == 'lat') {
    # ST_Y extracts latitude from each polygon vertex
    q1    <- base::paste("SELECT ST_Y( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lat FROM ( SELECT ST_Dump( ST_CollectionHomogenize( geom ) ) as geom FROM cb_2013_us_county_20m WHERE geoid = '", geoid, "') AS g", sep = "")
    coord <- pg.spi.exec(q1)
  }
  if (pos == 'lon') {
    # ST_X extracts longitude from each polygon vertex
    q1    <- base::paste("SELECT ST_X( ( ST_DumpPoints( ( g.geom ).geom ) ).geom ) as lon FROM ( SELECT ST_Dump( ST_CollectionHomogenize( geom ) ) as geom FROM cb_2013_us_county_20m WHERE geoid = '", geoid, "') AS g", sep = "")
    coord <- pg.spi.exec(q1)
  }
  return(coord)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_canopi_voronoi(geoid, tableName)
--
-- Builds a Voronoi tessellation over a county region using
-- weather station locations as seed points. Each Voronoi
-- polygon represents the area closest to a given station —
-- i.e. the "catchment zone" of that station.
--
-- The resulting polygons are stored in tableName and can be
-- visualized directly in QGIS by connecting to us_gis.
-- Used by the CANOPI vector-borne disease simulator to assign
-- spatial zones to epidemic model compartments.
--
-- Delegates entirely to ws2pgdb::canopiVoronoi() — no direct
-- DB access from R, all writes happen through pg.spi.exec
-- inside the package.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_canopi_voronoi(text, text)
  RETURNS text AS
$BODY$
  geoid     <- arg1   -- 5-digit county FIPS
  tableName <- arg2   -- destination table for Voronoi polygons
  return(ws2pgdb::canopiVoronoi(tableName, geoid))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_county_info(geoid, type, span)
--
-- Master orchestration function for the county data pipeline.
-- Calls five sub-functions in sequence and produces two output
-- tables used by the CANOPI epidemic simulator:
--
--   1. r_create_tiger_tracts_table  — Census tract geometries
--   2. r_create_midas_synth_hh_table — synthetic households
--   3. {tigertable}_clustered_by_nearest_ws
--        — each tract polygon merged to its nearest weather
--          station (used for spatial model assignment)
--   4. midas_pop_clustered_by_nearest_ws
--        — synthetic population aggregated per weather station
--          cluster (the key input to CANOPI)
--   5. ws_data_avg / ws_data_na
--        — actual NOAA weather observations for the span
--
-- The two clustering queries (q1, q2) use a spatial join:
-- ST_MakeLine + ST_Length finds the nearest station to each
-- tract centroid, then ST_UNION merges tract polygons into
-- station-level clusters.
--
-- All tables are idempotent — already-existing tables are
-- skipped, so the function is safe to re-run.
--
-- i.e. SELECT r_create_county_info('12087', 'TMAX', '10')
--        geoid='12087' = Monroe County FL
--        type='TMAX'   = maximum temperature
--        span='10'     = 10 years of weather data
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_county_info(text, text, text)
  RETURNS text AS
$BODY$

  geoid <- arg1   -- 5-digit county FIPS
  type  <- arg2   -- weather variable: TMAX, TMIN, PRCP
  span  <- arg3   -- number of years of weather data to retrieve
  ghcnd <- 'GHCND'

  # Step 1 — Retrieve weather stations and store their metadata
  # ws_metadata is the table name created by ws_metadata_span_2_pgdb
  stations    <- ws2pgdb::all_coor_ws(ghcnd, geoid, type)
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)

  # Step 2 — Download and load Census TIGER tract geometries
  # Returns empty string on failure (e.g. network error)
  tigertableName <- base::as.character(pg.spi.exec(sprintf("SELECT r_create_tiger_tracts_table('%1$s')", geoid)))

  if (identical(all.equal(tigertableName, ""), TRUE)) {
    return(tigertableName)
  }

  # Derive related table names from the tiger table name prefix
  table_cluster <- base::paste(tigertableName, "_clustered_by_nearest_ws", sep = "")

  # Step 3 — Download and load FRED synthetic households
  midastableName <- base::as.character(pg.spi.exec(sprintf("SELECT r_create_midas_synth_hh_table('%1$s')", geoid)))

  # Derive MIDAS population table name by substituting tiger_tracts → midas_pop
  temp      <- tigertableName
  midas_pop <- base::gsub("tiger_tracts", "midas_pop", temp)
  midas_pop_clustered_by_nearest_ws <- base::paste(midas_pop, "_clustered_by_nearest_ws", sep = "")

  # Step 4a — Cluster census tracts by nearest weather station
  # For each tract centroid, compute distance to every station (ST_Length
  # of the straight line), keep only the minimum, then UNION tract polygons
  # belonging to the same station into a single multi-polygon.
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

  # Step 4b — Cluster synthetic population by nearest weather station
  # Joins synthetic household counts (aggregated per census tract via
  # stcotrbg → geoid10 substring match) to the tract-to-station assignment
  # from step 4a. The result is one row per station with total household
  # population and merged polygon geometry.
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
        # DISTINCT ON (geoid) picks the single closest station per tract
        # abs(dist - min_dist) < epsilon guards against floating-point ties
        SELECT DISTINCT ON (gd.geoid) ws_id, t.geomLine, t.geom as poly, t.name, gd.geoid, gd.dist FROM geoidDistance gd, tractce t WHERE gd.geoid = t.geoid AND abs(gd.dist - t.dist) < 0.0000001\
      ),\
      ind_per_bg As(\
        # stcotrbg is a 14-char block-group FIPS; sum hh_size per block group
        SELECT stcotrbg As geoid, SUM(hh_size) As hh FROM ", midastableName, " GROUP BY stcotrbg ORDER BY stcotrbg\
      ),\
      ind_per_tract As(\
        # Collapse block groups to tract level (first 11 chars of stcotrbg = tract FIPS)
        SELECT substring( geoid for 11 ) As geoid, SUM(hh) As hh FROM ind_per_bg GROUP BY substring( geoid for 11) ORDER BY substring( geoid for 11)\
      ),
      syn_pop As(\
        # Join population counts to the tract-to-station spatial assignment
        SELECT ws_id, t.name, t.geoid, t.poly, midas.hh FROM ind_per_tract midas, line2Hub t WHERE midas.geoid = t.geoid\
      ),\
      cluster_pop As(\
        # Final aggregation: one row per weather station with total hh and merged polygon
        SELECT ws_id, name, sum(hh) as hh, ST_MULTI( ST_UNION(poly) ) as poly FROM syn_pop GROUP BY name, ws_id\
      )\
      SELECT * INTO ", midas_pop_clustered_by_nearest_ws, " FROM cluster_pop ORDER BY ws_id", sep = "")

  if (midasClusterExist) {
    print("Exists!")
  } else {
    pg.spi.exec(q2)
  }

  # Step 5 — Retrieve and store actual NOAA weather observations
  # ws_data_avg stores yearly averages; ws_data_na stores raw with NAs
  ws2pgdb::ws_data_avg_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  ws2pgdb::ws_data_na_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)

  # Return the name of the final population cluster table — primary output
  return(midas_pop_clustered_by_nearest_ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_midas_synth_hh_table(geoid)
--
-- Downloads the FRED synthetic population ZIP for a county,
-- extracts households.txt and people.txt (both tab-separated),
-- derives two fields missing from the new FRED format, and
-- loads the result into a county-specific household table.
--
-- Data source:
--   https://fred.publichealth.pitt.edu/proj/populations/usa/{geoid}.zip
--   (RTI International 2010 U.S. Synthetic Population v1, hosted by FRED)
--   Previously sourced from epimodels.org (now defunct).
--
-- Field derivation (fields absent from new households.txt):
--   hh_size — COUNT of persons per sp_hh_id in people.txt
--             (households.txt.sp_id = people.txt.sp_hh_id)
--   hh_age  — age of the household reference person (relate==0)
--             from people.txt; indicates head-of-household age
--
-- Output table schema (unchanged from original):
--   stcotrbg  CHAR(14)         — census block group FIPS (14 chars)
--   hh_race   SMALLINT         — household race code
--   hh_income DOUBLE PRECISION — household income
--   hh_size   SMALLINT         — number of persons in household
--   hh_age    SMALLINT         — age of reference person
--   longitude DOUBLE PRECISION — household longitude
--   latitude  DOUBLE PRECISION — household latitude
--
-- Table is idempotent: if it already exists, the name is returned
-- immediately without re-downloading.
--
-- i.e. SELECT r_create_midas_synth_hh_table('12087')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_midas_synth_hh_table(text)
  RETURNS text AS
$BODY$

  geoid <- arg1   -- 5-digit county FIPS

  # Build table name using county/state name prefix from TIGER lookup
  pre       <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  tableName <- paste(pre, "midas_synth_hh", sep = "")

  # Idempotent guard — skip download if table already loaded
  tableExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", tableName)))

  if (tableExist) {
    return(tableName)
  } else {
    # Create empty table with the expected schema before loading
    pg.spi.exec(sprintf("SELECT r_create_synth_hh_table_template('%1$s')", tableName))

    # FRED serves one ZIP per county, named by 5-digit FIPS
    zipFile  <- base::paste("/tmp/", geoid, ".zip", sep = "")
    download <- base::paste("https://fred.publichealth.pitt.edu/proj/populations/usa/", geoid, ".zip", sep = "")

    err <- try(utils::download.file(download, zipFile, method = "wget", quiet = TRUE))
    if (class(err) == "try-error") {
      return("")   -- empty string signals failure to caller
    }

    # ZIP extracts into a subfolder named by FIPS: {geoid}/households.txt
    hhFile     <- base::paste(geoid, "/households.txt", sep = "")
    peopleFile <- base::paste(geoid, "/people.txt",     sep = "")
    utils::unzip(zipFile, files = c(hhFile, peopleFile), exdir = "/tmp")

    # Load households — provides stcotrbg, hh_race, hh_income, latitude, longitude
    # Tab-separated (read.delim), not comma-separated as in the old epimodels format
    households <- utils::read.delim(base::paste("/tmp/", hhFile, sep = ""), head = TRUE)

    # Load people — needed only to derive hh_size and hh_age
    # sp_hh_id in people.txt is the foreign key to sp_id in households.txt
    people <- utils::read.delim(base::paste("/tmp/", peopleFile, sep = ""), head = TRUE)

    # Derive hh_size: count persons per household using sp_hh_id as grouping key
    hh_size        <- aggregate(sp_id ~ sp_hh_id, data = people, FUN = length)
    names(hh_size) <- c("sp_id", "hh_size")

    # Derive hh_age: age of household reference person (relate==0 = householder)
    # relate==1 is spouse, relate==2 is child, etc.
    ref_person        <- people[people$relate == 0, c("sp_hh_id", "age")]
    names(ref_person) <- c("sp_id", "hh_age")

    # Join both derived fields back to households on sp_id
    # all.x=TRUE preserves all households even if a match is missing
    out <- merge(households, hh_size,   by = "sp_id", all.x = TRUE)
    out <- merge(out,        ref_person, by = "sp_id", all.x = TRUE)

    # Select and order columns to match the table schema defined in the template
    out <- out[c("stcotrbg", "hh_race", "hh_income", "hh_size", "hh_age", "longitude", "latitude")]

    # Write to CSV and bulk-load via COPY — fastest PostgreSQL ingestion method
    updatedFile <- base::paste("/tmp/", geoid, "_synth_hh.csv", sep = "")
    utils::write.csv(out, file = updatedFile, row.names = FALSE)
    pg.spi.exec(sprintf("COPY \"%1$s\" FROM '%2$s' DELIMITER ',' CSV HEADER;", tableName, updatedFile))

    # Clean up all temporary files — /tmp would clear on restart anyway
    # but explicit cleanup prevents accumulation across multiple county calls
    unlink(zipFile)
    unlink(updatedFile)
    unlink(base::paste("/tmp/", geoid, sep = ""), recursive = TRUE)

    return(tableName)
  }

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_create_synth_hh_table_template(tableName)
--
-- Creates an empty household table with the standard schema
-- used by r_create_midas_synth_hh_table(). Called once before
-- the COPY bulk load to pre-define column types.
--
-- Schema mirrors the original MIDAS/FRED synthetic population
-- format: block-group FIPS + demographic attributes per household.
-- stcotrbg is 14 characters: 2 state + 3 county + 6 tract + 1 BG + 2 block.
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
-- r_create_table(var)
--
-- Creates a household table with the same schema as
-- r_create_synth_hh_table_template() but returns an integer
-- status code (0 = created, 1 = already exists) instead of
-- the table name. Used in contexts where idempotent creation
-- with a numeric return is more convenient.
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
    WHEN SQLSTATE '42P07' THEN RETURN 1;   -- 42P07 = duplicate_table
END;
$BODY$
  LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- r_create_tiger_tracts_table(geoid)
--
-- Downloads the statewide Census 2023 TIGER tract shapefile,
-- loads it via ogr2ogr into a staging table, filters to the
-- requested county, saves as a permanent table, and cleans up.
--
-- This is the internal counterpart to load_county_tracts().
-- load_county_tracts() is called interactively from manage.sh;
-- r_create_tiger_tracts_table() is called programmatically
-- from r_create_county_info() as part of the full pipeline.
--
-- Table naming: uses r_table_prefix() to build a human-readable
-- name like "texas_cameron_48061_tiger_tracts".
--
-- Idempotent: returns existing table name if already loaded.
-- Note: uses base::system2("ogr2ogr") without full path —
--   ogr2ogr must be on PATH inside the container (gdal-bin).
--
-- i.e. SELECT r_create_tiger_tracts_table('48061')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_create_tiger_tracts_table(text)
  RETURNS text AS
$BODY$

  geoid  <- arg1   -- 5-digit county FIPS

  # Build human-readable table name: e.g. "texas_cameron_48061_tiger_tracts"
  prefix       <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  pretableName <- base::paste(prefix, "tiger_tracts", sep = "")

  # Idempotent guard — skip download if table already loaded
  tableExist <- base::as.integer(pg.spi.exec(sprintf("SELECT r_table_exists('%1$s')", pretableName)))

  if (tableExist) {
    return(pretableName)
  } else {
    state_fips <- substr(geoid, 1, 2)
    county_fp  <- substr(geoid, 3, 5)

    # Census TIGER 2023 — statewide tract shapefile (all counties in state)
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

    # Load full statewide shapefile into staging via ogr2ogr Unix socket URI
    base::system2("ogr2ogr", args = c(
      "-f", "PostgreSQL",
      "postgresql://postgres@/us_gis?host=/var/run/postgresql",
      shp_path,
      "-nln", stg_tbl,
      "-nlt", "MULTIPOLYGON",
      "-overwrite"
    ))

    # Filter staging to requested county and save as named table
    pg.spi.exec(paste0("
      CREATE TABLE \"", pretableName, "\" AS
      SELECT * FROM ", stg_tbl, "
      WHERE statefp = '", state_fips, "' AND countyfp = '", county_fp, "'
    "))

    # Drop staging and clean up /tmp
    pg.spi.exec(paste0("DROP TABLE ", stg_tbl))
    unlink(zip_path)
    unlink(dir_path, recursive = TRUE)

    return(pretableName)
  }

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_avg_span_2_pgdb(ghcnd, geoid, type, span)
--
-- Retrieves NOAA weather data for a county over a given span
-- of years and stores yearly averages in the database.
-- Each weather station gets its own column in the output table.
-- Delegates to ws2pgdb package functions.
--
-- i.e. SELECT r_gen_data_avg_span_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_avg_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1   -- dataset ID, always 'GHCND'
  geoid    <- arg2   -- 5-digit county FIPS
  type     <- arg3   -- weather variable: TMAX, TMIN, PRCP
  span     <- arg4   -- number of years
  stations    <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_data     <- ws2pgdb::ws_data_avg_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_data)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_na_span_2_pgdb(ghcnd, geoid, type, span)
--
-- Same as r_gen_data_avg_span_2_pgdb() but stores raw daily
-- values with NA where data is missing rather than averages.
-- Used when the full time series (including gaps) is needed
-- for imputation or missing-data analysis.
--
-- i.e. SELECT r_gen_data_na_span_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_na_span_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations    <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  ws_metadata <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_data     <- ws2pgdb::ws_data_na_span_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_data)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_span_avg_2_pgdb(ghcnd, geoid, type, span)
--
-- Retrieves multi-year NOAA data and stores a single average
-- across the entire span (not per year). Used when a single
-- climatological mean is needed rather than a yearly series.
--
-- i.e. SELECT r_gen_data_span_avg_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_span_avg_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations    <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  stations    <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_metadata <- ws2pgdb::ws_data_span_avg_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_metadata)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_gen_data_span_na_2_pgdb(ghcnd, geoid, type, span)
--
-- Same as r_gen_data_span_avg_2_pgdb() but preserves NAs
-- across the full span rather than computing an average.
--
-- i.e. SELECT r_gen_data_span_na_2_pgdb('GHCND','12087','TMAX','10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_gen_data_span_na_2_pgdb(text, text, text, text)
  RETURNS text AS
$BODY$
  ghcnd    <- arg1
  geoid    <- arg2
  type     <- arg3
  span     <- arg4
  stations    <- as.data.frame(ws2pgdb::all_coor_ws(ghcnd, geoid, type))
  stations    <- ws2pgdb::ws_metadata_span_2_pgdb(geoid, type, stations, span)
  ws_metadata <- ws2pgdb::ws_data_span_na_2_pgdb(ghcnd, geoid, type, span, ws_metadata)
  return(ws_metadata)
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_population_size(geoid)
--
-- Returns the total synthetic household population for a county.
-- Prefers the clustered table (midas_pop_clustered_by_nearest_ws)
-- since it is already aggregated — a single SUM(hh) is faster
-- than summing hh_size across the full household table.
-- Falls back to midas_synth_hh if the cluster table doesn't
-- exist yet (i.e. r_create_county_info has not been run).
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_population_size(text)
  RETURNS integer AS
$BODY$

  geoid      <- arg1
  nameTable  <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))

  # Prefer the pre-aggregated cluster table for efficiency
  midasTable <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws", sep = "")
  exists     <- as.integer(pg.spi.exec(base::paste("SELECT r_table_exists('", midasTable, "')", sep = "")))

  if (exists) {
    total <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh) FROM ", midasTable, sep = "")))
  } else {
    # Fallback: sum individual household sizes from raw synthetic table
    midasTable <- base::paste(nameTable, "midas_synth_hh", sep = "")
    total      <- data.frame(pg.spi.exec(base::paste("SELECT SUM(hh_size) FROM ", midasTable, sep = "")))
  }
  return(total)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_pos(geoid, pos)
--
-- Returns latitude or longitude values for the weather stations
-- associated with a county. Hardcoded to TMAX variable and a
-- 3-year span — reads from the ws_metadata_span_3_tmax table.
-- Used when a quick positional lookup is needed without
-- specifying variable or span.
--
-- ST_Y extracts latitude from the PostGIS geometry column;
-- ST_X extracts longitude.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_pos(text, text)
  RETURNS SETOF double precision AS
$BODY$

  geoid <- arg1   -- 5-digit county FIPS
  pos   <- arg2   -- 'lat' or 'lon'

  # Hardcoded defaults — this function is a convenience shortcut
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
-- r_get_r_version()
--
-- Returns the R version string running inside PL/R.
-- Diagnostic utility — useful for confirming the R version
-- baked into the container image at build time.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_r_version()
  RETURNS text AS
$BODY$
  return(getRversion())
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_get_tract_size(geoid, wsNum)
--
-- Returns the household count (hh) assigned to the weather
-- station at position wsNum in the midas_pop_clustered table.
-- wsNum is 1-based (first station = 1); internally converted
-- to 0-based OFFSET for the SQL LIMIT/OFFSET query.
--
-- Used by the CANOPI simulator to determine the population
-- size of each spatial compartment (one per weather station).
-- Returns 0 if wsNum is 0 or the cluster table doesn't exist.
--
-- i.e. SELECT r_get_tract_size('12087', '3')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_get_tract_size(text, text)
  RETURNS integer AS
$BODY$

  geoid <- as.character(arg1)
  wsNum <- as.integer(arg2)

  # wsNum=0 is a sentinel for "no station" — return 0 immediately
  if (wsNum == 0) {
    return(0)
  }

  nameTable  <- data.frame(pg.spi.exec(base::paste("SELECT r_table_prefix('", geoid, "')", sep = "")))
  midasTable <- base::paste(nameTable, "midas_pop_clustered_by_nearest_ws ", sep = "")
  exists     <- as.integer(pg.spi.exec(base::paste("SELECT r_table_exists('", midasTable, "')", sep = "")))

  if (exists) {
    # Convert 1-based wsNum to 0-based OFFSET for LIMIT 1 OFFSET N
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
--
-- Returns the column name in a model output table that
-- corresponds to weather station wsNum. Model tables store
-- one column per weather station; this function resolves the
-- positional index to the actual column name string, quoted
-- for use in dynamic SQL.
--
-- Used by the CANOPI simulator to dynamically read model
-- results by station index without hardcoding column names.
--
-- i.e. SELECT r_model_colname('12087','10','dengue','4')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_model_colname(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid   <- as.character(arg1)
  span    <- as.character(arg2)
  disease <- arg3
  wsNum   <- as.integer(arg4)

  # Model table name: {prefix}ws_data_span_{span}_avg_tmax_{disease}
  prefix  <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_tmax_", disease, sep = "")

  # Read column names from information_schema, pick the wsNum-th column
  wsModel <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsModel <- wsModel[1:length(wsModel[,1]),]
  ws      <- base::paste("\"", wsModel[wsNum], "\"", sep = "")
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_model_values(geoid, span, disease, wsNum, type)
--
-- Returns the actual model values (time series) for the
-- weather station at position wsNum. Like r_model_colname()
-- but goes one step further and reads the column data.
--
-- type selects the weather variable suffix in the table name
-- (e.g. 'TMAX' → 'tmax'), allowing the same function to
-- retrieve results for different climate inputs.
--
-- i.e. SELECT r_model_values('12087','10','dengue','4','TMAX')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_model_values(text, text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid   <- base::as.character(arg1)
  span    <- base::as.character(arg2)
  disease <- base::as.character(arg3)
  wsNum   <- base::as.integer(arg4)
  type    <- base::tolower(base::as.character(arg5))   -- lowercase for table name suffix

  prefix  <- base::as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_", type, "_", disease, sep = "")

  # Resolve column name then read values from that column
  wsModel <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsModel <- wsModel[1:length(wsModel[,1]),]
  ws      <- base::paste("\"", wsModel[wsNum], "\"", sep = "")
  wsValue <- base::data.frame(pg.spi.exec(sprintf("SELECT %1$s FROM %2$s", ws, t1)))
  return(wsValue)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_name_2_fips(name, key)
--
-- Converts a geographic name to a FIPS code by querying the
-- 2013 Census cartographic boundary tables preloaded in us_gis.
--
-- key='st' queries cb_2013_us_state_20m  → returns state FIPS (2 digits)
-- key='co' queries cb_2013_us_county_20m → returns county FIPS (5 digits)
--
-- Note: county names are not unique across states (e.g. 'Monroe'
-- exists in many states). For counties, prefer querying with a
-- state filter or use the 5-digit FIPS directly.
--
-- i.e. SELECT r_name_2_fips('Monroe', 'co')
--      SELECT r_name_2_fips('Florida', 'st')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_name_2_fips(text, text)
  RETURNS text AS
$BODY$

  name <- arg1   -- geographic name, e.g. 'Monroe' or 'Florida'
  key  <- arg2   -- 'st' for state, 'co' for county

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
--
-- Converts a FIPS code to a state name. Accepts either a
-- 2-digit state FIPS or a 5-digit county FIPS (extracts the
-- first 2 digits in that case). Queries cb_2013_us_state_20m.
--
-- Used by r_table_prefix() and anywhere a human-readable
-- state name is needed from a FIPS input.
--
-- i.e. SELECT r_fips_2_state('12')      -- returns 'Florida'
--      SELECT r_fips_2_state('12087')   -- also returns 'Florida'
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_fips_2_state(text)
  RETURNS text AS
$BODY$

  geoid <- arg1
  # substr(geoid, 1, 2) works for both 2-digit and 5-digit FIPS
  nom   <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", substr(geoid, 1, 2)))
  return(nom)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_read_population_size(geoid)
--
-- Functionally identical to r_get_population_size() — reads
-- total synthetic population from MIDAS tables, preferring
-- the pre-aggregated cluster table for efficiency.
--
-- The duplication appears to be historical: r_get_population_size
-- was added later with the same logic. Both are kept for
-- backward compatibility with existing simulator call sites.
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
--
-- Returns the names of all weather stations stored for a county
-- from the ws_metadata table. Station names are the NOAA IDs
-- used as column headers in weather data tables.
--
-- i.e. SELECT r_read_ws('12087', 'TMAX', '10')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_read_ws(text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid <- arg1
  type  <- arg2
  span  <- arg3

  # ws_metadata table name: {prefix}ws_metadata_span_{span}_{type}
  nameTable     <- data.frame(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  metainfoTable <- base::paste(nameTable, "ws_metadata_span_", span, "_", type, sep = "")
  ws            <- data.frame(pg.spi.exec(sprintf("SELECT name FROM %1$s", metainfoTable)))
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_table_exists(var)
--
-- Returns 1 if table var exists, 0 if not. Uses a try/catch
-- on a SELECT to detect existence — SQLSTATE 42P01 is the
-- PostgreSQL error code for "undefined table".
--
-- Used throughout as an idempotent guard before creating tables.
-- Simpler than querying information_schema for this use case.
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
    WHEN SQLSTATE '42P01' THEN RETURN 0;   -- undefined_table
END;
$BODY$
  LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- r_table_prefix(geoid)
--
-- Generates a consistent, human-readable table name prefix
-- from a FIPS code by looking up state and county names from
-- the 2013 Census boundary tables.
--
-- For state FIPS (< 100):   "{state}_{geoid}_"
-- For county FIPS (5 digits): "{state}_{county}_{geoid}_"
--
-- Examples:
--   '12'    → "florida_12_"
--   '12087' → "florida_monroe_12087_"
--   '48061' → "texas_cameron_48061_"
--
-- Spaces and hyphens in names are replaced with underscores;
-- everything is lowercased for PostgreSQL compatibility.
-- All county-level tables in the schema share this prefix,
-- making it easy to identify and group related tables.
--
-- i.e. SELECT r_table_prefix('12087')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_table_prefix(text)
  RETURNS text AS
$BODY$

  geoid <- arg1

  if (as.integer(geoid) < 100) {
    # State-level prefix — just state name + FIPS
    state     <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", geoid))
    tableName <- base::paste(state, "_", geoid, "_", sep = "")
  } else {
    # County-level prefix — state + county + FIPS
    county    <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_county_20m WHERE GEOID='%1$s'", geoid))
    state     <- pg.spi.exec(sprintf("SELECT NAME FROM cb_2013_us_state_20m WHERE GEOID='%1$s'", substr(geoid, 1, 2)))
    tableName <- base::paste(state, "_", county, "_", geoid, "_", sep = "")
  }

  # Normalize: lowercase, replace spaces and hyphens with underscores
  varTable  <- tolower(tableName)
  tableName <- gsub(" ", "_", varTable)
  tableName <- gsub("-", "_", varTable)
  return(tableName)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_version()
--
-- Returns full R version information as a set of (name, value)
-- pairs — the same output as R's built-in version list.
-- Returns SETOF r_version_type (defined by the PLR extension).
--
-- Diagnostic utility for verifying the R build inside PL/R.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_version()
  RETURNS SETOF r_version_type AS
$BODY$
  cbind(names(version),unlist(version))
$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_voronoi(tableName, geomCol, idCol)
--
-- Computes a Voronoi tessellation over the points in tableName
-- using the deldir package. Each point (weather station) becomes
-- the seed of a polygon that contains all locations closer to
-- it than to any other seed.
--
-- The tessellation is clipped to a convex hull buffer at 50%
-- of the average span (x+y range / 2 * 0.50) to avoid infinite
-- polygons at the boundary. The buffer is computed from the
-- PostGIS convex hull of all input points.
--
-- Each polygon is intersected with the buffer to produce clean,
-- bounded geometry. Results are returned as (id, polygon) rows
-- matching r_voronoi_type.
--
-- Used by CANOPI to define spatial zones for the epidemic model.
-- Visualizable in QGIS by connecting to the us_gis database.
--
-- i.e. SELECT r_voronoi('ws_table', 'geom', 'id')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_voronoi(text, text, text)
  RETURNS SETOF r_voronoi_type AS
$BODY$
  library(deldir)

  # Read point coordinates from PostGIS geometry column
  points <- pg.spi.exec(
    sprintf("SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;", arg1, arg2)
  )

  # Buffer = 50% of average coordinate span — clips Voronoi to bounded region
  buffer_distance = ((abs(max(points$x) - min(points$x)) + abs(max(points$y) - min(points$y))) / 2) * (0.50)
  buffer_set <- pg.spi.exec(
    sprintf("SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;", arg1, arg2, buffer_distance)
  )

  # deldir computes the Delaunay triangulation + Voronoi tiles
  # rw extends the bounding box to prevent edge distortion
  voro = deldir(points$x, points$y, digits=22, frac=0.00000000000000000000000001, list(ndx=2,ndy=2),
    rw=c(min(points$x) - abs(min(points$x) - max(points$x)), max(points$x) + abs(min(points$x) - max(points$x)),
         min(points$y) - abs(min(points$y) - max(points$y)), max(points$y) + abs(min(points$y) - max(points$y))))
  tiles = tile.list(voro)

  poly = array()
  id   = array()
  p    = 1

  for (i in 1:length(tiles)) {
    tile = tiles[[i]]
    # Build WKT POLYGON string from tile vertices
    curpoly = "POLYGON(("
    for (j in 1:length(tile$x)) {
      curpoly = sprintf("%s %.6f %.6f,", curpoly, tile$x[[j]], tile$y[[j]])
    }
    # Close the ring by repeating the first vertex
    curpoly = sprintf("%s %.6f %.6f))", curpoly, tile$x[[1]], tile$y[[1]])

    # Intersect tile polygon with buffer to clip to bounded region
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
-- r_voronoi_scale(tableName, geomCol, idCol, scale)
--
-- Identical to r_voronoi() except the buffer multiplier is
-- provided as a parameter (arg4) instead of hardcoded at 0.50.
-- Useful when the default 50% buffer clips too aggressively
-- or leaves too much empty space around sparse station networks.
--
-- scale=0.50 reproduces r_voronoi() exactly.
-- scale=1.00 extends the buffer to the full average span.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_voronoi_scale(text, text, text, double precision)
  RETURNS SETOF r_voronoi_type AS
$BODY$
  library(deldir)

  points <- pg.spi.exec(
    sprintf("SELECT ST_X(%2$s) AS x, ST_Y(%2$s) AS y FROM %1$s;", arg1, arg2)
  )

  # arg4 replaces the hardcoded 0.50 scale factor
  buffer_distance = ((abs(max(points$x) - min(points$x)) + abs(max(points$y) - min(points$y))) / 2) * (arg4)
  buffer_set <- pg.spi.exec(
    sprintf("SELECT ST_Buffer(ST_Convexhull(ST_Union(%2$s)),%3$.6f) AS ewkb FROM %1$s;", arg1, arg2, buffer_distance)
  )

  voro = deldir(points$x, points$y, digits=22, frac=0.00000000000000000000000001, list(ndx=2,ndy=2),
    rw=c(min(points$x) - abs(min(points$x) - max(points$x)), max(points$x) + abs(min(points$x) - max(points$x)),
         min(points$y) - abs(min(points$y) - max(points$y)), max(points$y) + abs(min(points$y) - max(points$y))))
  tiles = tile.list(voro)

  poly = array()
  id   = array()
  p    = 1

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
--
-- Returns the column name for weather station wsNum in the
-- weather data span table (without disease suffix).
-- Similar to r_model_colname() but targets the raw weather
-- data table rather than the model output table.
-- Skips the first column (index column) by starting at index 2.
--
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

  # Weather data table (no disease suffix — raw station data)
  t1      <- base::paste(prefix, "ws_data_span_", span, "_avg_tmax", sep = "")

  # index 2 skips the first column (usually a date/time index)
  wsName  <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsName  <- wsName[2:length(wsName[,1]),]
  ws      <- base::paste("\"", wsName[wsNum], "\"", sep = "")
  return(ws)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_ws_values(geoid, span, type, wsNum)
--
-- Returns the weather data time series for station wsNum.
-- Like r_ws_colname() but reads the actual values from the
-- column. type selects the climate variable (TMAX, TMIN, PRCP).
--
-- i.e. SELECT r_ws_values('12087','10','TMAX','4')
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_ws_values(text, text, text, text)
  RETURNS SETOF text AS
$BODY$

  geoid  <- as.character(arg1)
  span   <- as.character(arg2)
  type   <- tolower(as.character(arg3))   -- lowercase for table name suffix
  wsNum  <- as.integer(arg4)

  prefix <- as.character(pg.spi.exec(sprintf("SELECT r_table_prefix('%1$s')", geoid)))
  t1     <- base::paste(prefix, "ws_data_span_", span, "_avg_", type, sep = "")

  # Resolve column name by index, skip first column (index)
  wsName <- base::data.frame(pg.spi.exec(sprintf("SELECT g.column_name FROM ( SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%1$s' ) as g", t1)))
  wsName <- wsName[2:length(wsName[,1]),]
  ws     <- base::paste("\"", wsName[wsNum], "\"", sep = "")

  # Read values from the resolved column
  wsValue <- data.frame(pg.spi.exec(sprintf("SELECT %1$s FROM %2$s", ws, t1)))
  return(wsValue)

$BODY$
  LANGUAGE plr;


-- ------------------------------------------------------------
-- r_check_libpaths()
--
-- Returns the R library search paths (.libPaths()) active
-- inside PL/R. Diagnostic utility for troubleshooting package
-- installation issues — confirms ws2pgdb and its dependencies
-- are visible to PL/R at runtime.
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.r_check_libpaths()
  RETURNS text AS
$BODY$
  return(.libPaths())
$BODY$
  LANGUAGE plr;
