from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import os

spark = SparkSession.builder \
    .appName("FileToKafka") \
    .getOrCreate()

ssc = StreamingContext(spark.sparkContext, batchDuration=60)

input_dir = "aggregated_data"

def send_to_kafka(rdd):
    if not rdd.isEmpty():
        df = spark.read.json(rdd.map(lambda x: x))
        df.selectExpr("CAST(value AS STRING)") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("topic", "aggregated_data") \
          .save()

file_stream = ssc.textFileStream(input_dir)

file_stream.foreachRDD(send_to_kafka)

ssc.start()
ssc.awaitTermination()