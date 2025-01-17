#!/bin/bash

# Check if the environment and version parameters are provided and valid
# $1: The environment parameter, which should be either "server" or "local".
# $2: The version parameter, which should be a positive integer.
if [ "$1" != "server" ] && [ "$1" != "local" ]; then
    echo "Usage: $0 [server|local] [version]"
    exit 1
fi

if [ -z "$2" ]; then
    transactions_table_name="transactions"
else
    transactions_table_name="$2"
fi

if [ -z "$3" ]; then
    hodls_table_name="hodls"
else
    hodls_table_name="$3"
fi

# Define the project directory based on the environment parameter
if [ "$1" == "server" ]; then
    PROJECT_DIR="/Users/tianzhichen/projects/btccore_reader"
else
    PROJECT_DIR="$HOME/projects/btccore_reader"
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$PROJECT_DIR/venv" ]; then
    /opt/homebrew/bin/python3.9 -m venv "$PROJECT_DIR/venv"
fi

# Activate virtual environment
source "$PROJECT_DIR/venv/bin/activate"

# Install requirements
pip install -r "$PROJECT_DIR/requirements.txt"

# Run main script
python "$PROJECT_DIR/timeseries.py" "$transactions_table_name" "$hodls_table_name"

# Deactivate virtual environment
deactivate
