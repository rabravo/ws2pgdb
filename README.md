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

Copy, paste, and execute the queries found in sql/functions.sql on the pgsql server.

#About Docker files
There has been an update where sql/functions.sql will be loaded in the container. No need to do nothing but running the dockerfile and create the image and run container

docker rm -f pg-gis-plr
docker build -t postgis-plr:3.5.2 docker/
docker run --name pg-gis-plr -e POSTGRES_PASSWORD=mysecretpassword -d postgis-plr:3.5.2
docker logs pg-gis-plr
docker exec -it pg-gis-plr psql -U postgres
docker stop pg-gis-plr
docker start pg-gis-plr


