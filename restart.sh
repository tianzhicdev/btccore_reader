#!bin/bash

git pull origin main

./launchctl_stop.sh
./launchctl_start.sh

