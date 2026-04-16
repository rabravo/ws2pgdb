# ws2pgdb

Package contains several functions that retrieve, filter, and store NOAA data in a local/remote Postgres database.
One useful function takes weather station locations in a region and constructs a Voronoi tessellation over that region (you will need a GIS software to visualize the output which is a set of geometries/polygons).
Another useful function iteratively requests several years of data from the NOAA digital warehouse. Since NOAA permits only one year of data requested at a time via the rnoaa package, it is particularly useful when many years of information are needed.

## Problems

Some system libraries are needed before you can start using ws2pgdb. Some of the methods necessitate the octave development tools, gdal, proj (`octave-dev`, `liboctave`, `libgdal`, `libproj-dev`). Try to install ws2pgdb and the installer will let you know when a library is needed. This may be a slow process but if you are using any Debian-like system, you can make use of the package manager:

```bash
apt-get install octave-dev liboctave-dev libgdal-dev libproj-dev
```

During a fresh installation, after a couple of months without touching these libraries, you may get dependency compatibility errors. To install these dependencies:

```r
devtools::install_github("rstats-db/RPostgres")
```

or

```bash
sudo su - -c "R -e \"install.packages('packagename', repos='http://cran.rstudio.com/')\""
```

## Census Data

Functions in this bundle request information from your local database. The database is assumed to contain U.S. Census data — cartographic boundary files a.k.a TIGER files in SHAPEFILE format. In the past QGIS was used to upload these files into the GIS-enabled Postgres database, but shapefiles can also be uploaded via command line using `psql` and `shp2pgsql`.

The data is available at:
https://www.census.gov/geo/maps-data/data/cbf/cbf_counties.html

For the original dissertation, TIGER files from 2013 were used: nation, state, county.

## PLR/SQL and PLPGSQL

The following functions enable communication between the vector-borne simulator and PostgreSQL.

Copy, paste, and execute the queries found in `sql/pg.spi.foo.sql` on the pgsql server.

## Docker Setup

The `sql/pg.spi.foo.sql` functions are automatically loaded into the container on first run. No manual steps needed — just pull the image from Docker Hub and run the container.

The image is available on Docker Hub at:
[https://hub.docker.com/r/drabravo/pg-gis-plr](https://hub.docker.com/r/drabravo/pg-gis-plr)

### Pull from Docker Hub (recommended)

```bash
docker pull drabravo/pg-gis-plr:latest
docker run --name pg-gis-plr -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d drabravo/pg-gis-plr:latest
docker logs pg-gis-plr
```

### Build locally from source

```bash
docker rm -f pg-gis-plr
docker build -t drabravo/pg-gis-plr:3.5.2 docker/
docker run --name pg-gis-plr -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d drabravo/pg-gis-plr:3.5.2
docker logs pg-gis-plr
```

### Connect to the database

```bash
docker exec -it pg-gis-plr psql -U postgres -d us_gis
```

### Stop and Start

```bash
docker stop pg-gis-plr
docker start pg-gis-plr
```

### Manage via script

```bash
./docker/manage.sh
```

## Loading County Tracts

Use the built-in `load_county_tracts()` function to download and load Census TIGER tract data for any US county:

```sql
SELECT load_county_tracts('48061');  -- Cameron County, TX
SELECT load_county_tracts('26125');  -- Oakland County, MI
```

Or use option **9** in `manage.sh` to be prompted for a FIPS code interactively.

## Loading Shapefiles Manually

With the port exposed (`-p 5432:5432`), `shp2pgsql` can be run locally and piped directly into the container.

Convert the shapefile to SQL, then load it into the database:

```bash
shp2pgsql -s 4269 -W UTF-8 tl_2010_48113_tract10.shp tl_2010_48113_tract10 us_gis > tl_2010_48113_tract10.sql
psql -h localhost -p 5432 -U postgres -d us_gis -f tl_2010_48113_tract10.sql
```

Or pipe directly in one step:

```bash
shp2pgsql -s 4269 -W UTF-8 tl_2010_48113_tract10.shp tl_2010_48113_tract10 us_gis | psql -h localhost -p 5432 -U postgres -d us_gis
```

The SRID (e.g. `4269`) can be found by opening the `.prj` file of your shapefile and pasting its content into https://spatialreference.org.

## Visualizing GIS Data with QGIS

[QGIS](https://qgis.org) is a free and open-source Geographic Information System that can connect directly to the `us_gis` PostgreSQL database to visualize spatial data such as county tracts, Voronoi tessellations, and weather station locations.

Download QGIS at: https://qgis.org/download/

To connect QGIS to the container:
1. Open QGIS
2. In the **Browser panel**, right-click **PostgreSQL** → **New Connection**
3. Enter the connection details below
4. Click **Test Connection** then **OK**
5. Browse and drag layers into the map canvas

## Client Connection Details

| Parameter | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `us_gis` |
| User | `postgres` |
| Password | `mysecretpassword` |
