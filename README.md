# San Francisco Crime Statistics with Spark Streaming 

## Project Overview 

In this project Udacity project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

You can try to answer the following questions with the dataset:

* What are the top types of crimes in San Fransisco?
* What is the crime density by location?

## Development environment 

You may choose to create your project in the workspace we provide here, or if you wish to develop your project locally, you will need to set up your environment properly as described below:

* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

## Step 1 
In this step we completed the following codes: 

[producer_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/producer_server.py)


[kafka_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/kafka_server.py)

![Image of kafka-consumer-console output](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/raw/main/img/screen_shot1.png)

## Step 2 
In this step we completed the following code: 

[data_stream.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/data_stream.py)

In the first time we run the above code as follows: 

<code>
    spark-submit --conf spark.ui.port=3000  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py  
</code>


![Image of kafka-consumer-console output](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/raw/main/img/screen_shot2.png)

![Image of kafka-consumer-console output](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/raw/main/img/screen_shot3.png)

![Image of kafka-consumer-console output](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/raw/main/img/screen_shot4.png)

## Step 3 

Write the answers to these questions in the README.md doc of your GitHub repo:

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

In order to answer the above questions we try different configs in SparkSesion. The results are shown in the table below. We use the value of processedRowsPerSecond as a means for measuring the throughput and latency of the data.  

|Configuration of the SparkSession property parameters |The processedRowsPerSecond is between|
|---|---|
|All SparkSession property parameters have the default values|0.36,0.64 |
|spark.driver.maxResultSize=1M| 0.14,0.69   |
|spark.driver.maxResultSize=10M|0.35, 1.15    |
|spark.driver.maxResultSize=100M|0.36, 0.99  |
|spark.executor.memory=500m|0.40, 0.89 |
|spark.executor.memory=600m|0.92, 1.05 |
|spark.executor.memory=700m|0.40, 0.66 |
|spark.executor.memory=800m|0.78, 0.88 |
|spark.executor.memory=900m|0.39,0.98 |
|spark.executor.logs.rolling.enableCompression=true|0.41, 1.07| 
|spark.executor.logs.rolling.enableCompression=false|0.09, 0.40| 
|spark.executor.logs.rolling.maxSize=90000|0.39, 1.11| 
|spark.executor.logs.rolling.maxSize=95000|0.86, 1.20| 
|spark.executor.logs.rolling.maxSize=100000|0.49, 1.11| 
|spark.python.worker.memory=100m|0.08, 0.44| 
|spark.python.worker.memory=200m|0.09, 0.58| 
|spark.python.worker.memory=300m|0.12 ,1.17| 
|spark.python.worker.memory=400m|0.39 ,1.20| 
|spark.python.worker.reuse=true|0.39 ,0.93| 
|spark.python.worker.reuse=false|0.39 ,1.02| 
|spark.reducer.maxSizeInFlight=20m|0.09 ,0.82| 
|spark.reducer.maxSizeInFlight=50m|0.46 ,1.01| 
|spark.reducer.maxSizeInFlight=60m|0.71 ,1.23| 
|spark.reducer.maxSizeInFlight=70m|0.55 ,1.075| 
|spark.shuffle.compress=true|0.42, 1.09| 
|spark.shuffle.compress=false|0.42, 1.08| 
|spark.shuffle.file.buffer=20k|0.42, 1.25| 
|spark.shuffle.file.buffer=30k|0.43, 1.24| 
|spark.shuffle.file.buffer=40k|0.43, 1.13| 
|spark.shuffle.file.buffer=50k|0.42, 1.11| 
|spark.shuffle.file.buffer=100k|0.98, 1.07| 
|spark.eventLog.compress=true|0.58, 1.04| 
|spark.eventLog.compress=false|0.41, 1.08| 



**Note**: In order to try configs in SparkSession we have to run 
data\_stream.py as follows(spark.driver.maxResultSize=1M): 

<code>
spark-submit --conf spark.ui.port=3000 --conf spark.driver.maxResultSize=1M --packages org.apache.spark:spark-sql-kafka-0-10\_2.11:2.3.4 --master local[*] data_stream.py
</code>

**Answers**:
1. In some cases the processedRowsPerSecond was affected as in the following SparkSession property parameters:
    * spark.driver.maxResultSize
    * spark.executor.logs.rolling.enableCompression
    * spark.python.worker.memory 
    * spark.reducer.maxSizeInFlight
    * spark.shuffle.file.buffer

But in other cases the processedRowsPerSecond was not affected as in the following SparkSession property parameters: 
    * spark.executor.memory
    * spark.executor.logs.rolling.maxSize
    * spark.python.worker.reuse
    * spark.shuffle.compress
    * spark.eventLog.compress

There are SparkSession property parameters that are related (those that end in compress) but only one of them affects the processedRowsPerSecond. 

2. The 2 most efficient SparkSession property key/value pairs are:     * spark.executor.logs.rolling.enableCompression
    * spark.python.worker.memory

It makes sense that the above SparkSession property parameters were the most optimal. The first enables executor log compression and the second controls the amount of memory to use per python worker process during aggregation. The only observation is that one would expect that if spark.executor.logs.rolling.enableCompression is set to false the values of the processedRowsPerSecond should be greater than when processedRowsPerSecond is set to true.  

## Files 
[consumer_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/consumer_server.py)

[producer_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/producer_server.py)

[kafka_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/kafka_server.py)

[data_stream.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/data_stream.py)

## References 

https://stackoverflow.com/questions/40282031/spark-executor-log-stderr-rolling

https://spark.apache.org/docs/latest/configuration.html

