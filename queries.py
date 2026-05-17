from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ── Spark + Delta setup ──────────────────────────────────────────
builder = (
    SparkSession.builder.appName("SentinelLake-Queries")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Load Delta table and register as SQL view ────────────────────
# This lets us query it using pure SQL strings
DELTA_PATH = "delta-table/network_logs"
df = spark.read.format("delta").load(DELTA_PATH)
df.createOrReplaceTempView("network_logs")
print("✅ Delta table loaded and registered as SQL view\n")

# ── Helper function ──────────────────────────────────────────────
def run_query(title, sql):
    print(f"{'='*60}")
    print(f"🔍 {title}")
    print(f"{'='*60}")
    spark.sql(sql).show(10, truncate=False)
    print()

# ── Query 1: SYN Flood Detection (Neptune-style) ─────────────────
# S0 flag = connection attempted, never completed
# High count of S0s from same service = SYN flood signature
run_query(
    "SYN Flood Detection — Services under SYN flood attack",
    """
    SELECT service,
           COUNT(*) as total_connections,
           SUM(CASE WHEN flag = 'S0' THEN 1 ELSE 0 END) as syn_flood_attempts,
           ROUND(SUM(CASE WHEN flag = 'S0' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as attack_percentage
    FROM network_logs
    GROUP BY service
    HAVING syn_flood_attempts > 100
    ORDER BY syn_flood_attempts DESC
    """
)

# ── Query 2: Port Sweep Detection ───────────────────────────────
# Port sweep = one source hitting many different services
# In our dataset, src context is captured via same_srv_rate
# Low same_srv_rate + high count = scanning many services
run_query(
    "Port Sweep Detection — Connections scanning multiple services",
    """
    SELECT service,
           protocol_type,
           COUNT(*) as connection_count,
           ROUND(AVG(same_srv_rate), 3) as avg_same_srv_rate,
           ROUND(AVG(dst_host_srv_count), 1) as avg_dst_srv_count
    FROM network_logs
    WHERE label IN ('portsweep', 'satan', 'nmap', 'ipsweep')
    GROUP BY service, protocol_type
    ORDER BY connection_count DESC
    LIMIT 10
    """
)

# ── Query 3: Brute Force Login Detection ────────────────────────
# Repeated failed logins = brute force signature
run_query(
    "Brute Force Detection — Services with repeated login failures",
    """
    SELECT service,
           COUNT(*) as total_attempts,
           SUM(num_failed_logins) as total_failed_logins,
           SUM(CASE WHEN logged_in = 0 THEN 1 ELSE 0 END) as sessions_not_logged_in,
           ROUND(AVG(num_failed_logins), 3) as avg_failures_per_connection
    FROM network_logs
    WHERE num_failed_logins > 0 OR label = 'guess_passwd'
    GROUP BY service
    ORDER BY total_failed_logins DESC
    """
)

# ── Query 4: High Volume DoS Detection ──────────────────────────
# DoS = massive traffic, src_bytes near zero (just hammering)
# serror_rate close to 1.0 = nearly all connections erroring
run_query(
    "DoS Attack Detection — Connections with DoS signatures",
    """
    SELECT label,
           COUNT(*) as count,
           ROUND(AVG(duration), 3) as avg_duration,
           ROUND(AVG(src_bytes), 1) as avg_src_bytes,
           ROUND(AVG(serror_rate), 3) as avg_serror_rate,
           ROUND(AVG(dst_host_serror_rate), 3) as avg_dst_error_rate
    FROM network_logs
    WHERE serror_rate > 0.8 AND src_bytes < 100
    GROUP BY label
    ORDER BY count DESC
    """
)

# ── Query 5: Privilege Escalation Detection ──────────────────────
# root_shell = 1 means attacker gained root access
# su_attempted = tried to switch to superuser
run_query(
    "Privilege Escalation — Connections that gained root access",
    """
    SELECT label,
           service,
           COUNT(*) as occurrences,
           SUM(root_shell) as root_shells_gained,
           SUM(su_attempted) as su_attempts,
           SUM(num_compromised) as total_compromised_conditions
    FROM network_logs
    WHERE root_shell = 1 OR su_attempted = 1
    GROUP BY label, service
    ORDER BY root_shells_gained DESC
    """
)

# ── Query 6: Overall Threat Summary ─────────────────────────────
run_query(
    "Threat Summary — Attack vs Normal traffic breakdown",
    """
    SELECT 
        CASE WHEN label = 'normal' THEN 'Normal Traffic'
             ELSE 'Attack Traffic' END as traffic_type,
        label,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM network_logs), 2) as percentage
    FROM network_logs
    GROUP BY label
    ORDER BY count DESC
    """
)

print("✅ All threat detection queries complete")
spark.stop()
