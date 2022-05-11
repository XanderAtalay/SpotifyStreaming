#!/bin/bash

python ./ProjectCode/APIStreaming.py

mysql --host=localhost --user=root --password=Xander22  < ./ProjectCode/CurrentFeaturedJoin.sql

brew services start mongodb-community@5.0

python ./ProjectCode/DataMerging.py

brew services stop mongodb-community@5.0
