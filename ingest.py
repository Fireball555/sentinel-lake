from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
import os

# ── Spark + Delta setup ──────────────────────────────────────────
builder = (
    SparkSession.builder.appName("SentinelLake-Ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Silence the WARN spam

# ── Column names for NSL-KDD ─────────────────────────────────────
# NSL-KDD has no header row, so we define the 42 columns manually
columns = [
    "duration", "protocol_type", "service", "flag",
    "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot",
    "num_failed_logins", "logged_in", "num_compromised", "root_shell",
    "su_attempted", "num_root", "num_file_creations", "num_shells",
    "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login",
    "count", "srv_count", "serror_rate", "srv_serror_rate",
    "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate",
    "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
    "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
    "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
    "dst_host_serror_rate", "dst_host_srv_serror_rate",
    "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
    "label"
]

# ── Load the CSV ─────────────────────────────────────────────────
DATA_PATH = "data/NSL_KDD_Train.csv"
DELTA_PATH = "delta-table/network_logs"

print(f"Loading data from {DATA_PATH}...")

df = spark.read.csv(DATA_PATH, header=False, inferSchema=True)

# Rename columns from default (_c0, _c1...) to our proper names
for i, col_name in enumerate(columns):
    df = df.withColumnRenamed(f"_c{i}", col_name)

print(f"Loaded {df.count()} rows with {len(df.columns)} columns")

# ── Quick sanity check ───────────────────────────────────────────
print("\nSample rows:")
df.select("protocol_type", "service", "flag", "src_bytes", "dst_bytes", "label").show(5)

print("\nAttack type distribution:")
df.groupBy("label").count().orderBy("count", ascending=False).show(20)

# ── Write to Delta Lake ──────────────────────────────────────────
print(f"\nWriting to Delta Lake at {DELTA_PATH}...")

df.write.format("delta").mode("overwrite").save(DELTA_PATH)

print("✅ Data written to Delta Lake")

# ── Verify we can read it back ───────────────────────────────────
print("\nVerifying read-back from Delta Lake...")
verify_df = spark.read.format("delta").load(DELTA_PATH)
print(f"✅ Read back {verify_df.count()} rows from Delta Lake")

spark.stop()
