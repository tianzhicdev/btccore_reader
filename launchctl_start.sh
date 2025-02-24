#!/bin/bash

echo "Loading btccore_reader service..."
if launchctl load /Users/tianzhichen/projects/btccore_reader/plist/com.user.btccore_reader.plist; then
    echo "btccore_reader service loaded successfully."
else
    echo "Failed to load btccore_reader service."
fi

echo "Loading timeseries service..."
if launchctl load /Users/tianzhichen/projects/btccore_reader/plist/com.user.timeseries.plist; then
    echo "timeseries service loaded successfully."
else
    echo "Failed to load timeseries service."
fi

echo "Loading backend service..."
if launchctl load /Users/tianzhichen/projects/btccore_reader/plist/com.user.backend.plist; then
    echo "backend service loaded successfully."
else
    echo "Failed to load backend service."
fi

echo "Loading difficulty service..."
if launchctl load /Users/tianzhichen/projects/btccore_reader/plist/com.user.difficulty.plist; then
    echo "difficulty service loaded successfully."
else
    echo "Failed to load difficulty service."
fi
