#!/bin/bash

# Check if the environment parameter is provided and valid
# $1: The environment parameter, which should be either "server" or "local".
if [ "$1" != "server" ] && [ "$1" != "local" ]; then
    echo "Usage: $0 [server|local]"
    exit 1
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
python "$PROJECT_DIR/transactions.py"

# Deactivate virtual environment
deactivate
