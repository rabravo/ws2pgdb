% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/all_coor_ws_2_pgdb.R
\name{all_coor_ws_2_pgdb}
\alias{all_coor_ws_2_pgdb}
\title{all_coor_ws_2_pgdb retrieves NOAA data and creates a table in a PG database.}
\usage{
all_coor_ws_2_pgdb(ghcnd, geoid, type, suffix)
}
\arguments{
\item{ghcnd}{String that refers to the Global Historical Climate Network (daily) dataset}

\item{geoid}{FIPS number from census tables}

\item{type}{Variable under investigation i.e. TMAX, TMIN, PRCP}

\item{suffix}{Substring to named the beginnig of Postgres table}
}
\value{
It returns the name of the table that was created or the string "one" when fail to find weather station information.
}
\description{
The function retrieves the coordinates (coor) longitude and latitude; geometry of that location;
name of NOAA weather stations (ws), as well as minimum and maximum date of available information.
It stores all the data with in a table in a Posgresql (pg) database (db) with POSTGIS extension 
installed on it. The function necessitates arguments: ghcnd, FIPS, type, geoid. The suffix argument
is used for naming the table in the database. The dbhost, dbport, dbname, dbuser, dbpasswd,
and noaa_token (key to NOAA service) are specified in a local file named pg_conf.yml that will 
be read in by yaml. See file format below. As it should be clear by now, this package assumes 
you have installed a Postgres database and the right permissions to create/read/write on it.
}
\note{
pg_config.yml has the following structure:
dbhost : Host Name (default localhost)
dbport : Port Number (default 5432)
dbname : Database Name
dbuser : User Name
dbpwd  : Password
token  : NOAA keypass

The output table name has the following form: st_co_geoid_suffix_ws_type
"st" is the U.S. State name and  "co" is the U.S. County name. The suffix and type are specified by the user.
The function assumes that a cb_2013_us_state_20m and a cb_2013_us_county_20m exist in the postgres database.
}
\examples{
all_coor_ws_2_pgdb(ghcnd = 'GHCND', geoid = '12087', type = 'PRCP', suffix = 'all_coor')

}
\keyword{NOAA,}
\keyword{PostgreSQL,}
\keyword{rgdal,}
\keyword{rnoaa,}
\keyword{rpostgis}
\keyword{stations,}
\keyword{weather}
