# ws2pgdb
Package contains several functions that retrieve, filter, and store NOAA data in a local/remote Postgres database. 
One useful function is that that takes weather station location in a region and constructs a Voronoi tessellation over that region (you will need a GIS software to visualize the output which is a set of geometries/polygones). 
Another useful function within the package is that that iteratively request several years of data from the NOAA digital warehouse. Since NOAA permits onle one year of data requested at a time, via the rnoaa package, it is particular useful when many years of information are needed. 

