import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaPythonObjectIngest") \
    .getOrCreate()

# Define the Kafka topic and Kafka broker address
kafka_topic = "pizzaCustomers"  # Replace with your Kafka topic name
kafka_broker = "localhost:9092"  # Replace with your Kafka broker address

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True)
])

# Read data from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) AS json_value")
kafka_stream = kafka_stream.select(from_json(kafka_stream["json_value"], schema).alias("data"))
kafka_stream = kafka_stream.select("data.id","data.name", "data.latitude", "data.longitude")

# Reduce the DataFrame to a single partition
kafka_stream = kafka_stream.coalesce(1)

output_directory = "C:\\Users\\MK078138\\SPA-assignment\\"
checkpoint = "C:\\Users\\MK078138\\spark-checkpoint"

query=kafka_stream.writeStream.format("csv").option("path",output_directory).option("startingOffsets","earliest").option("checkpointLocation", checkpoint).option("delimiter", ",").start()

# Wait for the query to complete (or you can schedule this task separately)
query.awaitTermination(timeout=10)