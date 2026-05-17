from groq import Groq
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
import os

# ── Load API key ─────────────────────────────────────────────────
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.3-70b-versatile"

# ── Spark + Delta setup ──────────────────────────────────────────
builder = (
    SparkSession.builder.appName("SentinelLake-LLM")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Load Delta table ─────────────────────────────────────────────
DELTA_PATH = "delta-table/network_logs"
df = spark.read.format("delta").load(DELTA_PATH)
df.createOrReplaceTempView("network_logs")
print("✅ Delta table loaded\n")

# ── Table schema we give to Groq ─────────────────────────────────
# This tells the LLM what columns exist and what they mean
# Without this, it's guessing. With this, it generates accurate SQL.
SCHEMA_CONTEXT = """
You are a security data analyst assistant. You help analyze network intrusion detection data.

The table is called 'network_logs' and has these key columns:
- duration: length of connection in seconds
- protocol_type: TCP, UDP, or ICMP
- service: network service (http, ftp, telnet, ssh, etc.)
- flag: connection status (SF=normal, S0=no response/SYN flood, REJ=rejected, RSTO=reset)
- src_bytes: bytes sent from source to destination
- dst_bytes: bytes sent from destination to source
- logged_in: 1 if successfully logged in, 0 if not
- num_failed_logins: number of failed login attempts
- root_shell: 1 if root shell was obtained (privilege escalation), 0 if not
- su_attempted: 1 if su command was attempted, 0 if not
- num_compromised: number of compromised conditions
- serror_rate: percentage of connections with SYN errors (high = SYN flood)
- same_srv_rate: percentage of connections to same service
- label: attack type (normal, neptune, satan, ipsweep, portsweep, smurf, nmap, 
         back, teardrop, warezclient, pod, guess_passwd, buffer_overflow, 
         warezmaster, land, imap, rootkit, loadmodule, ftp_write, multihop, phf, perl)

Attack type reference:
- neptune: SYN flood DoS attack
- satan/ipsweep/portsweep/nmap: network scanning/reconnaissance  
- smurf: ICMP flood DoS attack
- guess_passwd: brute force password attack
- buffer_overflow/rootkit/loadmodule/perl: privilege escalation attacks
- warezclient/warezmaster: unauthorized file transfer
- normal: legitimate traffic

IMPORTANT RULES:
1. Only return a single SQL SELECT statement. Nothing else.
2. No markdown, no backticks, no explanation.
3. Always use 'network_logs' as the table name.
4. Keep queries efficient - use LIMIT when appropriate.
5. Use ROUND() for percentages and decimals.
"""

# ── Core function: English → SQL → Result ───────────────────────
def ask(question: str):
    print(f"❓ Question: {question}")
    print("-" * 50)

    # Step 1: Ask Groq to convert English to SQL
    prompt = f"{SCHEMA_CONTEXT}\n\nConvert this question to SQL:\n{question}"
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        sql = response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ Groq API Error: {e}")
        print()
        return

    print(f"🔧 Generated SQL:\n{sql}")
    print("-" * 50)

    # Step 2: Run the SQL on Delta Lake
    try:
        result = spark.sql(sql)
        print("📊 Results:")
        result.show(15, truncate=False)
    except Exception as e:
        print(f"❌ SQL Error: {e}")
        print("The generated SQL had an error. Try rephrasing your question.")

    print()

# ── Test questions ───────────────────────────────────────────────
if __name__ == "__main__":
    # These are the kinds of questions a non-technical analyst would ask
    ask("Which services are most targeted by attacks?")
    ask("How many connections resulted in root access being gained?")
    ask("What percentage of total traffic is normal vs attacks?")
    ask("Show me all connection types where the attacker successfully logged in")

    spark.stop()
