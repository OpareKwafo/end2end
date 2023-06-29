#!/bin/bash

# load data from csv files into a database
echo "Creating raw database......."
python src/connect.py -db True -db_name "db1"

# Create table & upload data to database
echo "Creating table & uploading data......."
python src/connect.py -db_name 'db1' -tb_name "tb1" -fp "data\sales.csv"
