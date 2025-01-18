#!/bin/bash

echo "Unloading btccore_reader service..."
if launchctl unload /Users/tianzhichen/projects/btccore_reader/plist/com.user.btccore_reader.plist; then
    echo "btccore_reader service unloaded successfully."
else
    echo "Failed to unload btccore_reader service."
fi

echo "Unloading timeseries service..."
if launchctl unload /Users/tianzhichen/projects/btccore_reader/plist/com.user.timeseries.plist; then
    echo "timeseries service unloaded successfully."
else
    echo "Failed to unload timeseries service."
fi

echo "Unloading backend service..."
if launchctl unload /Users/tianzhichen/projects/btccore_reader/plist/com.user.backend.plist; then
    echo "backend service unloaded successfully."
else
    echo "Failed to unload backend service."
fi

echo "Unloading difficulty service..."
if launchctl unload /Users/tianzhichen/projects/btccore_reader/plist/com.user.difficulty.plist; then
    echo "difficulty service unloaded successfully."
else
    echo "Failed to unload difficulty service."
fi