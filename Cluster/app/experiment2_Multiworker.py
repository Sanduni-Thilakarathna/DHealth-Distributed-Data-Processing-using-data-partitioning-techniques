# Usage:
#   spark-submit /app/Experiment2/experiment2_partition_benchmark.py <mode> <runs> [--rewrite]
# Examples:
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment2_partition_benchmark.py directory 1 --rewrite
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment2_partition_benchmark.py hash 10
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment2_partition_benchmark.py range 15

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_timestamp
from pyspark.storagelevel import StorageLevel
import sys, time, csv, os

# ---------------- CLI ----------------
if len(sys.argv) < 3:
    print("Usage: spark-submit experiment2_partition_benchmark.py <mode: directory|hash|range> <runs> [--rewrite]")
    sys.exit(1)

mode = sys.argv[1].strip().lower()   # directory | hash | range
runs = int(sys.argv[2])
rewrite = any(arg == "--rewrite" for arg in sys.argv[3:])

if mode not in ("directory", "hash", "range"):
    print("Error: mode must be one of: directory | hash | range")
    sys.exit(2)

# -------------- CONFIG ---------------
FIXED_PARTITIONS = 4
OUTPUT_CSV = "/data/exp2_results.csv"                  # shared volume
SOURCE_CSV  = "file:///data/cleaned_personal_health_data.csv"
DIR_BASE    = "file:///data/output_directory_partitioned_exp2_userid_subset"  # used only in directory mode
LIMIT_USERS = int(os.environ.get("LIMIT_USERS", "0"))  # 0 = no limit; e.g., 200 for a quick, fair subset

spark = (
    SparkSession.builder
    .appName(f"Experiment2-{mode}")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", str(FIXED_PARTITIONS))
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.speculation", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"[INFO] mode={mode}, runs={runs}, rewrite={'YES' if rewrite else 'NO'}, partitions={FIXED_PARTITIONS}, LIMIT_USERS={LIMIT_USERS}")

# ----------- Load source CSV ----------
print("[STEP] Reading source CSV...")
df = spark.read.csv(SOURCE_CSV, header=True, inferSchema=True)
df = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))
df_small = df.select("User_ID", "Heart_Rate", "Timestamp")

# Keep the subset identical across all modes for fairness
if LIMIT_USERS > 0:
    user_subset = df_small.select("User_ID").distinct().limit(LIMIT_USERS)
    df_small = df_small.join(user_subset, on="User_ID", how="inner")
    print(f"[INFO] Using LIMIT_USERS={LIMIT_USERS} (same subset across modes)")

# -------- Build dfp per mode ----------
if mode == "directory":
    # Only write parquet folders when explicitly requested
    if rewrite:
        print(f"[STEP] Writing partitioned dataset to {DIR_BASE} (partitionBy=User_ID)...")
        (df_small
            .write
            .mode("overwrite")
            .option("mergeSchema", "false")
            .option("maxRecordsPerFile", 250000)
            .partitionBy("User_ID")
            .parquet(DIR_BASE))
    else:
        print("[STEP] Skipping rewrite (use --rewrite to rebuild directory data).")

    print(f"[STEP] Reading partitioned dataset from {DIR_BASE} ...")
    dfp = (spark.read
                .option("basePath", DIR_BASE)
                .parquet(f"{DIR_BASE}/*")
                .select("User_ID", "Heart_Rate"))
    # align degree and partitioning key
    dfp = dfp.repartition(FIXED_PARTITIONS, "User_ID")

elif mode == "hash":
    # Hash partitioning by key (default behavior of repartition(key))
    print("[STEP] Setting hash-based partitioning via repartition(User_ID)...")
    dfp = df_small.select("User_ID", "Heart_Rate").repartition(FIXED_PARTITIONS, "User_ID")

elif mode == "range":
    # Range partitioning by key
    print("[STEP] Setting range-based partitioning via repartitionByRange(User_ID)...")
    dfp = df_small.select("User_ID", "Heart_Rate").repartitionByRange(FIXED_PARTITIONS, col("User_ID"))

# --------- Cache, warm up ------------
dfp = dfp.persist(StorageLevel.MEMORY_AND_DISK)
print("[STEP] Warm-up (not timed)...")
_ = dfp.groupBy("User_ID").agg(avg("Heart_Rate").alias("Avg_Heart_Rate")).count()

# -------- Timed runs (actions) -------
print("[STEP] Timed runs...")
times = []
for i in range(runs):
    t0 = time.perf_counter()
    _ = dfp.groupBy("User_ID").agg(avg("Heart_Rate").alias("Avg_Heart_Rate")).count()
    elapsed = round(time.perf_counter() - t0, 4)
    print(f"[Run {i+1}/{runs}] {mode} (parts={FIXED_PARTITIONS}) -> {elapsed:.4f}s")
    times.append(elapsed)

avg_time = round(sum(times) / len(times), 4) if times else None
print(f"\nSUMMARY: mode={mode}, partitions={FIXED_PARTITIONS}, runs={runs}, avg={avg_time}s, times={times}")

# --------- Append to results CSV -----
file_exists = os.path.exists(OUTPUT_CSV)
with open(OUTPUT_CSV, "a", newline="") as f:
    w = csv.writer(f)
    if not file_exists:
        w.writerow(["Mode", "Partitions", "Runs", "Per-Run Times (s)", "Avg Exec Time (s)", "LimitUsers"])
    w.writerow([mode, FIXED_PARTITIONS, runs, ";".join(map(str, times)), avg_time, LIMIT_USERS])

spark.stop()
