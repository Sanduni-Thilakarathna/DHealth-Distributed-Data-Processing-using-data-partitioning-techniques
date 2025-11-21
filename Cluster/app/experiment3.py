# Usage:
#   spark-submit /app/Experiment2/experiment3_hybrid_partition_benchmark.py <mode> <runs> [--rewrite]
# Modes:
#   range-range | hash-hash | range-hash | directory | all
# Examples:
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment3_hybrid_partition_benchmark.py directory 1 --rewrite
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment3_hybrid_partition_benchmark.py range-hash 10
#   LIMIT_USERS=200 spark-submit /app/Experiment2/experiment3_hybrid_partition_benchmark.py all 15

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_timestamp
from pyspark.storagelevel import StorageLevel
import sys, time, csv, os

# ---------------- CLI ----------------
if len(sys.argv) < 3:
    print("Usage: spark-submit experiment3_hybrid_partition_benchmark.py <mode: range-range|hash-hash|range-hash|directory|all> <runs> [--rewrite]")
    sys.exit(1)

mode  = sys.argv[1].strip().lower()
runs  = int(sys.argv[2])
rewrite = any(arg == "--rewrite" for arg in sys.argv[3:])

VALID = {"range-range", "hash-hash", "range-hash", "directory", "all"}
if mode not in VALID:
    print(f"Error: mode must be one of {sorted(list(VALID))}")
    sys.exit(2)

# -------------- CONFIG ---------------
FIXED_PARTITIONS = 4
OUTPUT_CSV = "/data/exp3_results.csv"                      # shared volume
SOURCE_CSV  = "file:///data/cleaned_personal_health_data.csv"
DIR_BASE    = "file:///data/exp3_directory_userid"         # only used for 'directory'
LIMIT_USERS = int(os.environ.get("LIMIT_USERS", "0"))      # 0 = no limit; e.g., 200 for fair subset

spark = (
    SparkSession.builder
    .appName(f"Exp3-{mode}")
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

# Keep subset identical across all modes for fairness
if LIMIT_USERS > 0:
    user_subset = df_small.select("User_ID").distinct().limit(LIMIT_USERS)
    df_small = df_small.join(user_subset, on="User_ID", how="inner")
    print(f"[INFO] LIMIT_USERS={LIMIT_USERS} (same subset for all techniques)")

def build_dfp(tech: str):
    """Return a DataFrame partitioned according to the requested technique."""
    if tech == "directory":
        if rewrite:
            print(f"[STEP] Writing directory-partitioned dataset to {DIR_BASE} (partitionBy=User_ID)...")
            (df_small
                .write
                .mode("overwrite")
                .option("mergeSchema", "false")
                .option("maxRecordsPerFile", 250000)
                .partitionBy("User_ID")
                .parquet(DIR_BASE))
        else:
            print("[STEP] Skipping directory rewrite (use --rewrite to rebuild).")

        print(f"[STEP] Reading partitioned dataset from {DIR_BASE} ...")
        dfr = (spark.read
                    .option("basePath", DIR_BASE)
                    .parquet(f"{DIR_BASE}/*")
                    .select("User_ID", "Heart_Rate"))
        return dfr.repartition(FIXED_PARTITIONS, "User_ID")

    elif tech == "hash-hash":
        # hash partitioning by key (one shuffle; a second identical hash shuffle won't change much)
        print("[STEP] Applying hash-based partitioning via repartition(User_ID)...")
        return df_small.select("User_ID", "Heart_Rate") \
                       .repartition(FIXED_PARTITIONS, "User_ID")

    elif tech == "range-range":
        # range partitioning by key
        print("[STEP] Applying range-based partitioning via repartitionByRange(User_ID)...")
        return df_small.select("User_ID", "Heart_Rate") \
                       .repartitionByRange(FIXED_PARTITIONS, col("User_ID"))

    elif tech == "range-hash":
        # hybrid: first range partition, then hash repartition by same key
        print("[STEP] Applying hybrid partitioning: range → hash by User_ID...")
        tmp = df_small.select("User_ID", "Heart_Rate") \
                      .repartitionByRange(FIXED_PARTITIONS, col("User_ID"))
        return tmp.repartition(FIXED_PARTITIONS, "User_ID")

    else:
        raise ValueError(f"Unknown technique: {tech}")

def run_once(tech: str, n_runs: int):
    dfp = build_dfp(tech)
    # Cache base
    dfp = dfp.persist(StorageLevel.MEMORY_AND_DISK)
    print("[STEP] Warm-up (not timed)...")
    _ = dfp.groupBy("User_ID").agg(avg("Heart_Rate").alias("Avg_Heart_Rate")).count()

    # Timed runs
    print("[STEP] Timed runs...")
    times = []
    for i in range(n_runs):
        t0 = time.perf_counter()
        _ = dfp.groupBy("User_ID").agg(avg("Heart_Rate").alias("Avg_Heart_Rate")).count()
        elapsed = round(time.perf_counter() - t0, 4)
        print(f"[Run {i+1}/{n_runs}] {tech} (parts={FIXED_PARTITIONS}) -> {elapsed:.4f}s")
        times.append(elapsed)

    avg_time = round(sum(times) / len(times), 4) if times else None
    print(f"\nSUMMARY: technique={tech}, partitions={FIXED_PARTITIONS}, runs={n_runs}, avg={avg_time}s, times={times}")

    # Append to CSV
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["Technique", "Partitions", "Runs", "Per-Run Times (s)", "Avg Exec Time (s)", "LimitUsers"])
        w.writerow([tech, FIXED_PARTITIONS, n_runs, ";".join(map(str, times)), avg_time, LIMIT_USERS])

# Run for requested mode
if mode == "all":
    for tech in ["range-range", "hash-hash", "range-hash", "directory"]:
        run_once(tech, runs)
else:
    run_once(mode, runs)

spark.stop()
