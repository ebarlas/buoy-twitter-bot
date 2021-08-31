### Overview

Buoy Twitter Bot is a Python project for powering a Twitter bot in the form of an 
AWS Lambda Function. Historical buoy observations are stored in an Amazon DynamoDB table.
The project was designed and implemented to run entirely on the AWS free tier.
All buoy observations come from [NOAA endpoints](#noaa-endpoints).

### NOAA Endpoints

The following NOAA buoy observation endpoints are used.
* Last 5 days - `https://www.ndbc.noaa.gov/data/5day2/{buoy}_5day.txt`
* Last 45 days - `https://www.ndbc.noaa.gov/data/realtime2/{buoy}.txt`
* Month of current year - `https://www.ndbc.noaa.gov/stdmet/{month_name}/{buoy}.txt`
* Month of specific year - `https://www.ndbc.noaa.gov/view_text_file.php?filename={buoy}{month_id}{year}.txt.gz&dir=data/stdmet/{month_name}/`
* Year - `https://www.ndbc.noaa.gov/view_text_file.php?filename={buoy}h{year}.txt.gz&dir=data/historical/stdmet/`

### DynamoDB Table Structure
* Partition key `id`, type string
* Range key `time`, type string
* Local secondary index `id-month`, range key `month`, type number, projection `waveheight`
* Local secondary index `id-monthday`, range key `monthday`, type string, projection `waveheight`

### DynamoDB Table Attributes
* `id`* (S) - NOAA buoy ID
* `time`** (S) - buoy observation time in the form `YYYYMMDDHH`
* `year` (N) - year integer
* `month` (N) - month integer
* `day` (N) - day integer
* `hour` (N) - hour integer
* `minute` (N) - minute integer
* `monthday` (S) - month-day in the form `MMDD`
* `yearmonth` (S) - year-month in the form `YYMM`
* `waveheight` (N) - wave height floating point number
* `wavedir` (N) - wave direction integer
* `domperiod` (N) - dominant period floating point number
* `avgperiod` (N) - average period floating point number

\* for embedded year-month max aggregations, `id` `{buoy}/yearmonth` is used  
\** for embedded year-month max aggregations, `time` `YYYYMM` is used 

### DynamoDB Queries
* Find latest
  * Partition key is target buoy
  * Range scan backward (`ScanIndexForward`=`False`)
  * Limit 1
  * Items queried: 1
* Find max wave height
  * Query embedded year-month max aggregations
  * Partition key is `{buoy}/yearmonth`
  * Range scan over _all_ items
  * Retain maximum 
  * Items queried: total number of months
* Find last occurrence
  * First, query embedded year-month max aggregations
    * Partition key is `{buoy}/yearmonth`
    * Range scan backward (`ScanIndexForward`=`False`)
    * Stop when sufficiently large wave height encountered
    * Items queried: number of months until occurrence
  * Second, query granular items
    * Partition key is target buoy
    * Range key is `begins_with(time, YYYYMM)`
    * Range scan backward 
    * Stop when sufficiently larger wave height encountered
    * Items queried: up to hours-per-month 
* Find month percentile
  * Partition key is target buoy
  * Index `id-month`
  * Range scan over _all_ items
  * Sort results and find insertion point for percentile
  * Items queried: hours-per-month * years-in-db or 1/12 of _all_ items
* Find month-year percentile
  * Partition key is target buoy
  * Index `id-monthyear`
  * Range scan over _all_ items
  * Sort results and find insertion point for percentile
  * Items queried: hours-per-day * years-in-db


### Command Line Applications

The applications below [NOAA endpoints](#noaa-endpoints). 
* `loadlast5.py` - fetches last 5 days of observations and stores in a DynamoDB table
* `loadlast45.py` - fetches last 45 days of observations and stores in a DynamoDB table
* `loadmonth.py` - fetches month of observations and stores in a DynamoDB table
* `loadyearmonths.py` - fetches months of observations in a particular year and stores in a DynamoDB table
* `loadyears.py` - fetches multiple years of observations and stores in a DynamoDB


### Data Oddities

* 5 download endpoints (5-day, 45-day, previous month, year-month, year)
* 3 slightly different csv formats (YY, #YY, YYYY)
* Multiple notations for missing or unfulfilled column (99.99, MM, etc)
* Multiple orders (time ascending, time descending)
* Multiple precisions ("real-time" has one-tenth precison, "historical" has one-hundredth)

### Building

Docker provides a convenient way to build a package tailored for the 
AWS Lambda [Python runtime](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html).

At the time of this writing, the `amazonlinux` image available on Docker Hub
reflects the Lambda Python runtime.

Prepare Docker container:
```
docker pull amazonlinux
docker run --rm --name buoy -v "$PWD":/usr/src/awslambda -w /usr/src/awslambda -it amazonlinux bash
```

Run `build.sh` script:
```
yum install python3
yum install zip
./build.sh 
```