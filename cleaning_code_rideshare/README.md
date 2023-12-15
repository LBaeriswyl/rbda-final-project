# hadoop-mapreduce

## Data

### Description

****
The For-Hired Vehicle Trip Dataset for New York City contains detailed information on each ride trip (see below data fields) of all for-hired vehicles from Feb 2019 to Nov 2022.

The dataset are available in **parquet** format. There are in total 46 parquet files, each of which contains all the trip data recorded in a month.

```
💡 From here on, a ride trip is referred as a record
```

### Source

NYC Uber/Lyft Trip Data (2019-2022), Size: 19.23 GB,
source: <https://www.kaggle.com/datasets/jeffsinsel/nyc-fhvhv-data>

### Example instance of a record from raw data

```
hvfhs_license_num: HV0005
dispatching_base_num: B02510
request_datetime: 1549663629000000
pickup_datetime: 1549663819000000
dropoff_datetime: 1549665378000000
PULocationID: 62
DOLocationID: 76
trip_miles: 5.3
trip_time: 1499
base_passenger_fare: 21.62
tolls: 0.0
bcf: 0.54
sales_tax: 1.92
congestion_surcharge: 0.0
tips: 0.0
driver_pay: 18.67
shared_request_flag: N
shared_match_flag: Y
access_a_ride_flag: N
wav_request_flag: N
```

### Example instance of a record from cleaned and augemented data

```
hvfhs_license_num: HV0005
request_datetime: 2019-02-08 17:07:09
pickup_datetime: 2019-02-08 17:10:19
dropoff_datetime: 2019-02-08 17:36:18
PULocationID: 62
DOLocationID: 76
trip_miles: 5.3
trip_time: 1499
base_passenger_fare: 21.62
tolls: 0.0
bcf: 0.54
sales_tax: 1.92
congestion_surcharge: 0.0
tips: 0.0
total_charges: 24.08
wait_time: 190
```

## How to Reproduce

### Prerequisites

Make sure you have [kaggle](https://github.com/Kaggle/kaggle-api) installed and add your kaggle api key to your machine.

### Run

```bash
chmod +x run.sh
./run.sh
```

## Directory structure

```
.
├── README.md
├── REPORT.pdf
├── pom.xml
├── run.sh
└── src
    └── main
        └── java
            ├── Clean.java
            ├── CleanMapper.java
            ├── Profile.java
            ├── ProfileCombiner.java
            ├── ProfileMapper.java
            └── ProfileReducer.java
```
