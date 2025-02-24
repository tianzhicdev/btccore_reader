#!/bin/bash

git pull origin main

pg_ctl -D /Volumes/4tb0/postgres_data restart

./launchctl_stop.sh
./launchctl_start.sh

launchctl list | grep com.user

