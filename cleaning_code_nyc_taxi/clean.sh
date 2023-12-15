# Compile Java code
javac -classpath $(hadoop classpath) *.java

# Create a JAR file
jar cvf DataCleaner.jar *.class

# Remove old Hadoop output
hadoop fs -rm -r -f project/output/$1*

# Run MapReduce
hadoop jar DataCleaner.jar DataCleaner project/$1_combined.csv project/output/$1 $1


# Display the output of the third iteration
hadoop fs -getmerge project/output/$1 ./$1_output.csv
