# Compile Java code
javac -classpath $(hadoop classpath) *.java

# Create a JAR file
jar cvf DataProfiler.jar *.class

# Remove old Hadoop output
hadoop fs -rm -r -f project/profiling*

# Run MapReduce
hadoop jar DataProfiler.jar DataProfiler project/output/$1/ project/profiling/$1 $2

# Fetch output to local disk
hadoop fs -getmerge project/profiling/$1 ./$1_$2.csv
