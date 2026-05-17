import streamlit as st
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from groq import Groq
from dotenv import load_dotenv
import os

# ── Page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="SentinelLake",
    page_icon="🛡️",
    layout="wide"
)

# ── Load env ─────────────────────────────────────────────────────
load_dotenv()

# ── Schema context (same as llm_query.py) ───────────────────────
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

Attack types:
- neptune: SYN flood DoS attack
- satan/ipsweep/portsweep/nmap: network scanning/reconnaissance
- smurf: ICMP flood DoS attack
- guess_passwd: brute force password attack
- buffer_overflow/rootkit/loadmodule/perl: privilege escalation attacks
- warezclient/warezmaster: unauthorized file transfer
- normal: legitimate traffic

RULES:
1. Return ONLY a single SQL SELECT statement. Nothing else.
2. No markdown, no backticks, no explanation.
3. Always use 'network_logs' as the table name.
4. Use LIMIT 20 by default unless the question asks for more.
5. Use ROUND() for percentages and decimals.
"""

# ── Initialize Spark (cached so it only starts once) ────────────
@st.cache_resource
def init_spark():
    builder = (
        SparkSession.builder.appName("SentinelLake-UI")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.format("delta").load("delta-table/network_logs")
    df.createOrReplaceTempView("network_logs")
    return spark

# ── Initialize Groq (cached) ─────────────────────────────────────
@st.cache_resource
def init_groq():
    return Groq(api_key=os.getenv("GROQ_API_KEY"))

# ── SQL generation ───────────────────────────────────────────────
def generate_sql(client, question):
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": SCHEMA_CONTEXT},
            {"role": "user", "content": f"Convert this to SQL: {question}"}
        ],
        temperature=0.1
    )
    return response.choices[0].message.content.strip()

# ── UI ───────────────────────────────────────────────────────────
st.title("🛡️ SentinelLake")
st.caption("Security Analytics Lakehouse — Powered by Delta Lake + LLM")

# Sidebar with stats
with st.sidebar:
    st.header("📊 Dataset Info")
    st.metric("Total Connections", "125,973")
    st.metric("Attack Traffic", "46.54%")
    st.metric("Normal Traffic", "53.46%")
    st.metric("Attack Types", "21")
    st.metric("Features per Connection", "42")

    st.divider()
    st.header("💡 Example Questions")
    examples = [
        "Which services are most targeted by attacks?",
        "How many connections gained root access?",
        "Show me all brute force attack connections",
        "What percentage of neptune attacks used TCP?",
        "Which protocol is most used by attackers?",
        "Show me connections with failed logins on telnet",
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state.question = ex

# Main area
st.subheader("Ask a question about your security logs")

question = st.text_input(
    "Type your question in plain English",
    value=st.session_state.get("question", ""),
    placeholder="e.g. Which services are most targeted by attacks?"
)

if st.button("🔍 Analyze", type="primary") and question:
    with st.spinner("Generating SQL and querying Delta Lake..."):
        try:
            spark = init_spark()
            groq_client = init_groq()

            # Generate SQL
            sql = generate_sql(groq_client, question)

            # Show SQL in expandable section
            with st.expander("🔧 Generated SQL", expanded=True):
                st.code(sql, language="sql")

            # Run query
            result_df = spark.sql(sql)
            pandas_df = result_df.limit(20).toPandas()

            # Show results
            st.subheader("📊 Results")
            st.dataframe(pandas_df, use_container_width=True)
            st.caption(f"Returned {len(pandas_df)} rows")

        except Exception as e:
            st.error(f"Error: {e}")
            st.info("Try rephrasing your question.")
