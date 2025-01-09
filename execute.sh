#!/bin/bash

# Define the project directory
PROJECT_DIR="/Users/tianzhichen/projects/btccore_reader"

# Create virtual environment if it doesn't exist
if [ ! -d "$PROJECT_DIR/btccore_reader" ]; then
    python3 -m venv "$PROJECT_DIR/btccore_reader"
fi

# Activate virtual environment
source "$PROJECT_DIR/btccore_reader/bin/activate"

# Install requirements
pip install -r "$PROJECT_DIR/requirements.txt"

# Run main script
python "$PROJECT_DIR/main.py"

# Deactivate virtual environment
deactivate
