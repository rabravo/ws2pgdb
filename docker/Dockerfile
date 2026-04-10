# Use the official PostgreSQL 17 image as the base
FROM postgres:17

# Install dependencies for building PostGIS and PL/R
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    postgresql-17-plr \
    libgdal-dev \
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

# Copy the initialization script to the proper directory
COPY --chmod=755 docker/init-db.sh /docker-entrypoint-initdb.d/init-db.sh
