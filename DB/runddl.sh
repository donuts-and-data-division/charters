#!/bin/bash

psql -h 127.0.0.1 -f chartersDDL.sql testdb -U cappuser

sudo service postgresql restart