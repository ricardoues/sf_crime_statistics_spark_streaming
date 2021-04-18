import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import time


# TODO Create a schema for incoming resources
#schema = StructType([StructField("status", StringType(), True), 
#                     StructField("timestamp", TimestampType(), True)
#])

schema = StructType([StructField("crime_id", StringType(), True), 
                     StructField("original_crime_type_name", StringType(), True), 
                     StructField("report_date", TimestampType(), True), 
                     StructField("call_date", TimestampType(), True ),
                     StructField("offense_date", TimestampType(), True ),
                     StructField("call_time", DateType(), True ),
                     StructField("call_date_time", TimestampType(), True ),
                     StructField("disposition", StringType(), True ),
                     StructField("address", StringType(), True ),
                     StructField("city", StringType(), True ),
                     StructField("state", StringType(), True ),
                     StructField("agency_id", StringType(), True ),
                     StructField("address_type", StringType(), True ),
                     StructField("common_location", StringType(), True )
])



def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    
    
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.sanfranciscopolice.crime.statistics") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 10) \
        .option("maxOffsetsPerTrigger", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    

    # TODO select original_crime_type_name and disposition
    distinct_table =  service_table.selectExpr('original_crime_type_name', 'disposition', 'to_timestamp(call_date_time) as call_date_time')
    
    distinct_table.printSchema()
    
    # The following url was very helpful.
    # https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html

    # count the number of original crime type
    agg_df = distinct_table \
            .withWatermark("call_date_time", '60 minutes') \
            .groupBy(distinct_table.original_crime_type_name, distinct_table.disposition, psf.window("call_date_time", "10 minutes", "5 minutes") ) \
            .count()
            

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    
    
    query = agg_df \
            .writeStream \
            .format("console") \
            .trigger(processingTime="1 seconds") \
            .queryName("counts") \
            .outputMode("append") \
            .option("truncate", "false") \
            .start()
    
                

    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"    
    radio_code_df = spark.read.option("multiline", True).json(radio_code_json_filepath)
        
    # The above line raises the message the following error:
    # |-- _corrupt_record: string (nullable = true) 
    # in order to fix it, the following web page was helpful:
    # https://stackoverflow.com/questions/38895057/reading-json-with-apache-spark-corrupt-record
    
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    radio_code_df.printSchema()

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition) \
                 .writeStream \
                 .format("console") \
                 .trigger(processingTime="1 seconds") \
                 .queryName("join") \
                 .outputMode("append") \
                 .option("truncate", "false") \
                 .start()

                       
    
    # TODO attach a ProgressReporter
    time.sleep(30)
    query.awaitTermination()
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
