#!/bin/bash

psql -d capp30254_project1 -U capp30254_project1_user -h pg.rcc.uchicago.edu -f combine_nces_tables.sql
