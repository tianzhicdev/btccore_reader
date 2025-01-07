#!/bin/bash

# Create virtual environment if it doesn't exist
if [ ! -d "btccore_reader" ]; then
    python3 -m venv btccore_reader
fi

# Activate virtual environment
source btccore_reader/bin/activate

# Install requirements
pip install -r requirements.txt

# Run main script
python main.py

# Deactivate virtual environment
deactivate
