#SELECT User_ID, AVG(Heart_Rate) FROM data GROUP BY User_ID

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hash, date_format, to_timestamp
import sys
import time
import csv
import os

# Command-line args: partitioning type, num partitions
if len(sys.argv) != 3:
    print("Usage: spark-submit experiment1_multiworker.py <partitioning_type> <num_partitions>")
    sys.exit(1)

partitioning_type = sys.argv[1].lower()
num_partitions = int(sys.argv[2])

# Setup Spark
spark = SparkSession.builder \
    .appName("Experiment1-MultiWorker") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("file:///data/cleaned_personal_health_data.csv", header=True, inferSchema=True)
df = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

# Apply partitioning
if partitioning_type == "hash":
    df = df.withColumn("hash_partition", (hash(col("User_ID")) % num_partitions))
    df = df.repartition(num_partitions, "hash_partition")

elif partitioning_type == "range":
    df = df.withColumn("range_partition", date_format("Timestamp", "yyyy-MM"))
    df = df.repartition(num_partitions, "range_partition")

elif partitioning_type == "directory":
    df.write.partitionBy("User_ID").mode("overwrite").parquet("file:///data/output_directory_partitioned")
    df = spark.read.parquet("file:///data/output_directory_partitioned")
else:
    print("Invalid partitioning type: choose from hash, range, directory")
    sys.exit(1)

# Run query & time it
start_time = time.time()
result = df.groupBy("User_ID").agg(avg("Heart_Rate").alias("Avg_Heart_Rate"))
result.collect()
end_time = time.time()

execution_time = end_time - start_time
print(f"Partitioning Type: {partitioning_type}, Partitions: {num_partitions}, Execution Time: {execution_time:.4f} seconds")

# After computing execution_time:
output_path = "/data/exp1_results.csv"
file_exists = os.path.exists(output_path)

with open(output_path, "a", newline="") as csvfile:
    writer = csv.writer(csvfile)
    if not file_exists:
        writer.writerow(["Partitioning Type", "Partitions", "Execution Time (s)"])
    writer.writerow([partitioning_type, num_partitions, round(execution_time, 4)])

# Stop Spark
spark.stop()
