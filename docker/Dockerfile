# Use the official PostgreSQL 17 image as the base
FROM postgres:17

# Install dependencies for building PostGIS and PL/R
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    postgresql-17-plr \
    libgdal-dev \
    gdal-bin \
    libproj-dev \
    libgeos-dev \
    libxml2-dev \
    libjson-c-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    wget \
    cmake \
    git \
    postgresql-common && \
    apt-get clean

# Install PostGIS 3.5.2
RUN wget https://download.osgeo.org/postgis/source/postgis-3.5.2.tar.gz && \
    tar -xvzf postgis-3.5.2.tar.gz && \
    cd postgis-3.5.2 && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf postgis-3.5.2 postgis-3.5.2.tar.gz

# Configure PostgreSQL to enable PL/R and PostGIS extensions
RUN echo "shared_preload_libraries = 'plr,postgis-3'" >> /usr/share/postgresql/postgresql.conf.sample

# Install system dependencies for R devtools
RUN apt-get update && apt-get install -y \
    r-base \
    r-base-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev && \
    apt-get clean

# Install remotes, rnoaa (archived from CRAN, must use GitHub), and ws2pgdb
RUN apt-get update && apt-get install -y libgit2-dev && apt-get clean && \
    R -e "install.packages('remotes', repos='https://cloud.r-project.org')" && \
    R -e "remotes::install_github('ropensci/rnoaa')" && \
    R -e "remotes::install_github('rabravo/ws2pgdb')"

# Copy the initialization script to the proper directory
COPY --chmod=755 init-db.sh /docker-entrypoint-initdb.d/init-db.sh
