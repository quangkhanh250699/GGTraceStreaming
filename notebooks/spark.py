 
#import findspark
#findspark.init()

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, udf, split, mean
from pyspark.sql import Row, DataFrame
 
packages = ["org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"]
packages = ",".join(packages)

master = "spark://localhost:7077"
appName = "sparkTest"

spark = SparkSession.builder \
                    .master(master) \
                    .appName(appName) \
                    .config("spark.jars.packages", packages) \
                    .config("spark.executor.memory", "1024m") \
                    .config("spark.executor.core", "1") \
                    .getOrCreate()

print(spark)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092, kafka:9093") \
  .option("subscribe", "TASK-EVENT") \
  .option("startingOffsets", "latest") \
  .load()

value = df.select(col("value").cast("string").alias("Value"))
split_col = split(col("Value"), ",")
extract_task = value.select(split_col.getItem(0).cast("int").alias('id'),
                            split_col.getItem(5).cast("int").alias("machineId"),
                            split_col.getItem(6).cast("int").alias("eventType"),
                            split_col.getItem(9).cast("int").alias("priority"),
                            split_col.getItem(10).cast("float").alias("cpuRequest"),
                            split_col.getItem(11).cast("float").alias("memoryRequest"),
                            split_col.getItem(12).cast("float").alias("diskspaceRequest"))
mean_query = extract_task.select(mean(col("cpuRequest")).alias("mean_cpu_request"),
                                 mean(col("memoryRequest")).alias("mean_memory_request"),
                                 mean(col("diskspaceRequest")).alias("mean_diskspace_request")) \
                         .writeStream \
                         .format("memory") \
                         .queryName("mean") \
                         .outputMode("complete") \
                         .start()
