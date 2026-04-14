#!/bin/bash

IMAGE_NAME="postgis-plr:3.5.2"
CONTAINER_NAME="pg-gis-plr"
DB_PASSWORD="mysecretpassword"
DB_PORT="5432"

show_menu() {
    echo ""
    echo "==============================="
    echo "   pg-gis-plr Docker Manager   "
    echo "==============================="
    echo "1) Rebuild image"
    echo "2) Create container"
    echo "3) Start container"
    echo "4) Stop container"
    echo "5) Recreate container (stop + rm + create)"
    echo "6) Show container logs"
    echo "7) Connect to database (psql)"
    echo "8) Exit"
    echo "==============================="
    echo -n "Select an option: "
}

rebuild_image() {
    echo "Rebuilding image $IMAGE_NAME..."
    docker build -t "$IMAGE_NAME" "$(dirname "$0")"
}

create_container() {
    echo "Creating container $CONTAINER_NAME..."
    docker run --name "$CONTAINER_NAME" \
        -e POSTGRES_PASSWORD="$DB_PASSWORD" \
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
    docker exec -it "$CONTAINER_NAME" psql -U postgres
}

while true; do
    show_menu
    read -r option
    case $option in
        1) rebuild_image ;;
        2) create_container ;;
        3) start_container ;;
        4) stop_container ;;
        5) recreate_container ;;
        6) show_logs ;;
        7) connect_db ;;
        8) echo "Bye!" ; exit 0 ;;
        *) echo "Invalid option, try again." ;;
    esac
done
