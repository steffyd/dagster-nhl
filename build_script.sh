#!/bin/bash
rebuild_all=false
for arg in "$@"
do
    if [[ $arg == "--rebuild-all" ]]; then
        # set the rebuild-all variable to true
        rebuild_all=true
    fi
done

# if rebuild-all is not set, then ask the user if they want to rebuild all images
if [[ $rebuild_all == false ]]; then
    read -p "Do you want to recreate all images? (y/n): " recreate_all
fi
# set rebuild_all to true if the user wants to recreate all images
if [[ $recreate_all == "y" ]]; then
    rebuild_all=true
fi

# set the Dagster version we want to use here
dagster_version="DAGSTER_VERSION=1.7.7"

# Check the user's input and rebuild the images accordingly
if [[ $rebuild_all == true ]]; then
    # Rebuild all images
    echo "Recreating all images..."
    # Rebuild all images in the dagit and dagster-daemon folders
    # use the dagster_version variable to set the Dagster version in the build-args
    docker build --platform=linux/amd64 --no-cache -t steffyd/dagster_dagit:latest --build-arg $dagster_version ./dagit
    docker build --platform=linux/amd64 --no-cache -t steffyd/dagster_daemon:latest --build-arg $dagster_version ./dagster-daemon
fi
# Always rebuild only the dagster-code image
echo "Recreating dagster-code image..."
# Rebuild the dagster-code image
# use the dagster_version variable to set the Dagster version in the build-args
docker build --platform=linux/amd64 --no-cache -t steffyd/dagster_code:latest --build-arg $dagster_version ./dagster-code

# Push the rebuilt images to the correct location
echo "Pushing images to the correct location..."
# Push the rebuilt images to the correct location
docker push steffyd/dagster_dagit:latest
docker push steffyd/dagster_daemon:latest
docker push steffyd/dagster_code:latest

echo "Build script completed."