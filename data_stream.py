import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import time


# We create a schema for incoming resources


schema = StructType([StructField("crime_id", StringType(), True), 
                     StructField("original_crime_type_name", StringType(), True), 
                     StructField("report_date", StringType(), True), 
                     StructField("call_date", StringType(), True ),
                     StructField("offense_date", StringType(), True ),
                     StructField("call_time", StringType(), True ),
                     StructField("call_date_time", StringType(), True ),
                     StructField("disposition", StringType(), True ),
                     StructField("address", StringType(), True ),
                     StructField("city", StringType(), True ),
                     StructField("state", StringType(), True ),
                     StructField("agency_id", StringType(), True ),
                     StructField("address_type", StringType(), True ),
                     StructField("common_location", StringType(), True )
])


def run_spark_job(spark):

    # We create Spark Configuration
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
    

    # We show schema for the incoming resources for checks
    df.printSchema()

    # We extract the correct column from the kafka input resources
    # We take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    

    # We select original_crime_type_name and disposition
    distinct_table =  service_table.selectExpr('original_crime_type_name', 'disposition', 'to_timestamp(call_date_time) as call_date_time')
    
    
    
    
    distinct_table.printSchema()
    
    # The following url was very helpful.
    # https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html

    # count the number of original crime type
    # The below resource was very helpful.
    # https://knowledge.udacity.com/questions/457255
    
    
    agg_df = distinct_table \
            .withWatermark("call_date_time", '5 minutes') \
            .groupBy(distinct_table.original_crime_type_name, distinct_table.disposition, psf.window("call_date_time", "2 minutes", "1 minutes") ) \
            .count()
    
    
    

    # We submit a screen shot of a batch ingestion of the aggregation.
    # We write output stream.
    
    
    query = agg_df \
            .writeStream \
            .format("console") \
            .trigger(processingTime="3 seconds") \
            .queryName("counts") \
            .outputMode("append") \
            .option("truncate", False) \
            .start()
    
                

    # We get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"    
    radio_code_df = spark.read.option("multiline", True).json(radio_code_json_filepath)
        
    # The following url was very helpful 
    # https://stackoverflow.com/questions/38895057/reading-json-with-apache-spark-corrupt-record
    
    radio_code_df.printSchema()

    # We clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # We rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    radio_code_df.printSchema()

    # We join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition) \
                 .writeStream \
                 .format("console") \
                 .trigger(processingTime="3 seconds") \
                 .queryName("join") \
                 .outputMode("append") \
                 .option("truncate", False) \
                 .start()

                       
    
    # We attach a ProgressReporter
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
        

    # The following does not work
    #spark.config("spark.ui.port", 3000)
    # In order to use the Spark Streaming UI, the following reference was very useful
    # https://stackoverflow.com/questions/55057822/set-spark-configuration/55057977
    # We have to run data_stream.py as follows 
    # spark-submit --conf spark.ui.port=3000  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py 
    # In order to user various configs in SparkSession 
    # we have to run data_streamp.py as follows 
    # spark-submit --conf spark.ui.port=3000 --conf spark.driver.maxResultSize=1M --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py 
    
    
    # The following urls were very helpful in order to get the 
    # SparkSession property parameters.     
    # https://stackoverflow.com/questions/43024766/what-are-sparksession-config-options
    # https://stackoverflow.com/questions/5618878/how-to-convert-list-to-string
    
    print(spark.sparkContext.getConf().getAll())
    

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
