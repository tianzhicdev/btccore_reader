#!/bin/bash

if [ "$1" != "server" ] && [ "$1" != "local" ]; then
    echo "Usage: $0 [server|local] [version]"
    exit 1
fi

if [ -z "$2" ]; then
    difficulty_table_name="difficulty"
else
    difficulty_table_name="$2"
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

# Check if requirements.txt exists before attempting to install
if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    pip install -r "$PROJECT_DIR/requirements.txt"
else
    echo "Error: requirements.txt not found in $PROJECT_DIR"
    deactivate
    exit 1
fi

# Run main script
if [ -f "$PROJECT_DIR/difficulty.py" ]; then
    python "$PROJECT_DIR/difficulty.py" "$difficulty_table_name"
else
    echo "Error: difficulty.py not found in $PROJECT_DIR"
    deactivate
    exit 1
fi

# Deactivate virtual environment
deactivate
