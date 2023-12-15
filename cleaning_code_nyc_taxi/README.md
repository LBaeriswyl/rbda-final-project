To run MapReduce code for cleaning the NYC Taxi data, run `./clean.sh <color>`, where `color` can be either `yellow` or `green`. This will run the entire cleaning procedure for the dataset corresponding to the color passed as an argument. It is assumed that the data on HDFS is stored in `project/<color>_combined.csv`.

The Hive and Trino queries are stored in `nyc_<color>_taxi_data_commands.sql`