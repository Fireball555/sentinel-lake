# SentinelLake — Findings & Reflections

## Why I Built This

I kept running into the same wall: cybersecurity problems at any real scale
are fundamentally data problems. I knew how attacks worked conceptually,
but I had no idea how large companies actually store, query, and analyze
millions of security events. Delta Lake was my answer to that question.
This project is me figuring out how big companies do this.

## What The Data Showed

### The Network Is A Scary Place
46.54% of all traffic in this dataset is malicious. Nearly half.
That's not a minor threat — that's a network under constant siege.
It reinforced why I'm studying cybersecurity in the first place.

### The Most Common Attack Isn't The Most Dangerous One
Neptune accounts for 32.72% of all traffic — SYN flood attacks
hammering services into unavailability. We have 41,000 examples of it.
Any detection system trained on this data will get very good at
spotting Neptune.

Buffer overflow has 30 rows.

That imbalance is the real security problem. After seeing 41,000
Neptune attacks, a system gets sharp at catching them. But buffer
overflow — which gives an attacker full code execution on your machine —
barely shows up. Rarer attacks are harder to detect and almost always
more dangerous. I'd lose sleep over the 30-row attacks, not the
41,000-row ones.

### 169 Connections Gained Root Access
These are the ones that matter most. Not probes, not scans —
actual successful compromises where an attacker obtained root shell.
In a real environment, my first move would be to investigate every
single one of those 169 connections: what did they do after getting in,
did they persist, what did they touch.

### The courier Service Is 81% Under Attack
Out of 734 connections to the courier service, 596 were SYN flood
attempts. In a real environment that service gets taken offline
immediately while the source is investigated.

### Data Quality Is A Real Problem
Query 5 showed "normal" labeled connections with root_shell = 1.
Legitimate admin activity and a successful attacker look identical
in raw logs. This is called ground truth uncertainty — one of the
hardest unsolved problems in security data science.

## What This Project Can't Do Yet

**No mitigations.** Detection without response is half the job.
A production system needs automated responses — block the IP,
isolate the host, alert the SOC. This project finds the attacks
but doesn't stop them.

**It needs real data.** NSL-KDD is a research dataset from a
simulated environment. Real production traffic is messier, faster,
and full of edge cases this dataset doesn't capture.

**It needs ML.** Rule-based detection catches known attack patterns.
It's completely blind to novel attacks — things an attacker does
slightly differently to evade the rules. A proper production system
layers ML on top of rules to catch anomalies that don't match any
known signature.

## What I Actually Learned

Before this, I knew how attacks worked. Now I know how you watch
for them at scale. The gap between "I understand this attack" and
"I can detect this attack across a million events" turns out to be
an entire engineering discipline — data engineering, lakehouse
architecture, SQL analytics, LLM integration.

This project is the bridge between those two things for me.
