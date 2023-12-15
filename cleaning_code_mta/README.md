To compile and run the MapReduce code for cleaning the turnstile data, please navigate to the Cleaning/ folder and run

	javac -classpath `hadoop classpath` *.java
	jar cvf MTATurnstileDataCleaning.jar *.class
	rm *.class -f
	hadoop jar MTATurnstileDataCleaning.jar MTATurnstileDataCleaning <input-HDFS-dir> <output-HDFS-dir>

To compile and run the MapReduce code for cleaning the geographic station data, please navigate to the `MTA-Geo-Station-Cleaning/` folder and run the same commands as the above, with `MTATurnstileDataCleaning.jar` replaced by `MTAGeoStationCleaning.jar`.

To compile and run the MapReduce code for profiling the turnstile data, please navigate to the `Profiling/` folder and run the first three commands in similar ways. For the fourth command, please choose the appropriate driver class for the profiling you wish to execute.


For Hive and Trino queries, please consult the instructions for the corresponding topic in the `SQL-Commands/` folder.
