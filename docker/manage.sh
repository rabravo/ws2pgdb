#!/bin/bash

IMAGE_NAME="drabravo/pg-gis-plr:3.5.2"
CONTAINER_NAME="pg-gis-plr"
DB_PASSWORD="mysecretpassword"
DB_PORT="5432"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

show_menu() {
    echo ""
    echo "==============================="
    echo "   pg-gis-plr Docker Manager   "
    echo "==============================="
    echo "1) Fetch latest from GitHub"
    echo "2) Rebuild image"
    echo "3) Create container"
    echo "4) Start container"
    echo "5) Stop container"
    echo "6) Recreate container (stop + rm + create)"
    echo "7) Show container logs"
    echo "8) Connect to database (psql)"
    echo "9) Load county tracts by FIPS"
    echo "10) Exit"
    echo "==============================="
    echo -n "Select an option: "
}

fetch_project() {
    echo "Fetching latest from GitHub..."
    git -C "$REPO_DIR" pull
}

rebuild_image() {
    echo "Rebuilding image $IMAGE_NAME..."
    docker build -t "$IMAGE_NAME" "$(dirname "$0")"
}

create_container() {
    echo "Creating container $CONTAINER_NAME..."
    docker run --name "$CONTAINER_NAME" \
        -e POSTGRES_PASSWORD="$DB_PASSWORD" \
        -e NOAA_TOKEN="$NOAA_TOKEN" \
        -p "$DB_PORT":5432 \
        -d "$IMAGE_NAME"
}

start_container() {
    echo "Starting container $CONTAINER_NAME..."
    docker start "$CONTAINER_NAME"
}

stop_container() {
    echo "Stopping container $CONTAINER_NAME..."
    docker stop "$CONTAINER_NAME"
}

recreate_container() {
    echo "Recreating container $CONTAINER_NAME..."
    docker stop "$CONTAINER_NAME" 2>/dev/null
    docker rm "$CONTAINER_NAME" 2>/dev/null
    create_container
}

show_logs() {
    echo "Logs for $CONTAINER_NAME..."
    docker logs "$CONTAINER_NAME"
}

connect_db() {
    echo "Connecting to database..."
    docker exec -it "$CONTAINER_NAME" psql -U postgres -d us_gis
}

load_county_tracts() {
    echo -n "Enter 5-digit county FIPS code (e.g. 48061 for Cameron TX, 26125 for Oakland MI): "
    read -r fips
    if [[ ! "$fips" =~ ^[0-9]{5}$ ]]; then
        echo "Invalid FIPS code. Must be exactly 5 digits."
        return
    fi
    echo "Loading county tracts for FIPS $fips..."
    docker exec -it "$CONTAINER_NAME" psql -U postgres -d us_gis -c "SELECT load_county_tracts('$fips');"
}

while true; do
    show_menu
    read -r option
    case $option in
        1) fetch_project ;;
        2) rebuild_image ;;
        3) create_container ;;
        4) start_container ;;
        5) stop_container ;;
        6) recreate_container ;;
        7) show_logs ;;
        8) connect_db ;;
        9) load_county_tracts ;;
        10) echo "Bye!" ; exit 0 ;;
        *) echo "Invalid option, try again." ;;
    esac
done
