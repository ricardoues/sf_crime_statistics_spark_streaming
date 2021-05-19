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

|Configuration of the SparkSession property parameters |The result is between|
|---|---|
|All SparkSession property parameters have the default values|0.36,0.64 |
|spark.driver.maxResultSize=1M| 0.14,0.69   |


## Files 
[consumer_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/consumer_server.py)

[producer_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/producer_server.py)

[kafka_server.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/kafka_server.py)

[data_stream.py](https://github.com/ricardoues/sf_crime_statistics_spark_streaming/blob/main/data_stream.py)

