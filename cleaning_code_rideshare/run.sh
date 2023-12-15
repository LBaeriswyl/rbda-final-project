# download the data
echo "Downloading data..."
# kaggle datasets download -d jeffsinsel/nyc-fhvhv-data
# unzip nyc-fhvhv-data.zip -d data
# mv data/fhvhv_tripdata_2019-02.parquet raw_data/

RAW_SOURCE_LOCAL_DIR=data/raw_data
BASE_HDFS_DIR=project
RAW_INPUT_HDFS_DIR=$BASE_HDFS_DIR/raw_data
RAW_OUTPUT_HDFS_DIR=$BASE_HDFS_DIR/cleaned
PROFILE_OUTPUT_HDFS_DIR=$BASE_HDFS_DIR/profile
LOCAL_BUILD_DIR=target

# upload the data
echo "Uploading data to HDFS..."
hadoop fs -test -d $RAW_INPUT_HDFS_DIR
if [ $? == 0 ]; then
    echo "$RAW_INPUT_HDFS_DIR already exists"
else
    echo "Copying $RAW_SOURCE_LOCAL_DIR to $RAW_INPUT_HDFS_DIR"
    hadoop fs -mkdir -p $RAW_INPUT_HDFS_DIR
    hadoop fs -put $RAW_SOURCE_LOCAL_DIR $BASE_HDFS_DIR
fi


# build the source
echo "Building the source..."
mvn clean install
mvn package

# run the job
echo "Running the cleaning job..."
hadoop fs -rm -r $RAW_OUTPUT_HDFS_DIR
start=$(date +%s)
hadoop jar $LOCAL_BUILD_DIR/clean.jar $RAW_INPUT_HDFS_DIR $RAW_OUTPUT_HDFS_DIR
end=$(date +%s)
echo "clean job run for $((end - start)) sec"

# profile the data

echo "Profiling the data..."
hadoop fs -rm -r $PROFILE_OUTPUT_HDFS_DIR

start=$(date +%s)
hadoop jar $LOCAL_BUILD_DIR/profile.jar $RAW_OUTPUT_HDFS_DIR $PROFILE_OUTPUT_HDFS_DIR
end=$(date +%s)
echo "hadoop run for $((end - start)) sec"

hadoop fs -cat $PROFILE_OUTPUT_HDFS_DIR/* | hadoop fs -put - $PROFILE_OUTPUT_HDFS_DIR/merge
hadoop fs -cat $PROFILE_OUTPUT_HDFS_DIR/merge
