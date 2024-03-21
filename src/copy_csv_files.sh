#!/bin/bash

source_dir="./csv_files"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # for Linux
    dest_dir="/var/lib/postgresql/data_folder"
    sudo mkdir -p "$dest_dir"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # for macOS
    dest_dir="/Users/$(whoami)/data_folder"
    mkdir -p "$dest_dir"
fi

if [ -d "$source_dir" ]; then
    if [ -d "$dest_dir" ]; then
        cp "$source_dir"/*.csv "$dest_dir"
        echo "CSV files copied from $source_dir to $dest_dir"
    else
        echo "Destination directory $dest_dir does not exist."
        exit 1
    fi
else
    echo "Source directory $source_dir does not exist."
    exit 1
fi