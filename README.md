StreamSentinel 🚨

> An agentic AI system that monitors real-time Kafka streams, 
> detects anomalies autonomously, and either fixes them or 
> escalates to a human — powered by 5 specialized AI agents.

**Built by a senior backend engineer. No tutorials followed. 
Production-grade architecture.**

---

## What is StreamSentinel?

Most pipeline failures don't announce themselves.

They show up as wrong numbers in a dashboard 3 hours later. 
Or a 2am page about data that's been corrupted since midnight.

StreamSentinel puts 5 AI agents on top of your Kafka streams 
to catch failures before any human notices — and either fix 
them automatically or wake up the right person with full context.

---

## The 5 Agents

| Agent | Role | Tech |
|-------|------|------|
| **WatcherAgent** | Monitors live Kafka streams for anomalies | Python, kafka-python |
| **DiagnosisAgent** | Uses LLM to explain root cause in plain English | Llama 3.2, Ollama |
| **BlastRadiusAgent** | Scores downstream impact deterministically | Python, graph traversal |
| **RemediationAgent** | Auto-fixes LOW/MEDIUM, escalates HIGH to human | Python |
| **NarratorAgent** | Writes plain-English incident post-mortem | Llama 3.2, Markdown |

---

## Architecture
Live Kafka Stream
↓
WatcherAgent    → detects anomaly
↓
DiagnosisAgent  → LLM explains why
↓
BlastRadiusAgent → scores impact LOW/MEDIUM/HIGH
↓
RemediationAgent → auto-fix or escalate
↓
NarratorAgent   → writes incident report
↓
Back to watching...

---

## Key Design Decisions

**Why deterministic blast radius scoring (no AI)?**

The decision of whether to auto-fix or wake up a human must be 
predictable and auditable. LLMs introduce randomness. 
A graph traversal doesn't. Governance requires determinism.

**Why Kafka-native (not Airflow)?**

Every existing self-healing pipeline project wraps Airflow DAGs. 
StreamSentinel runs directly on Kafka consumer groups — watching 
live streams, not scheduled jobs. That's architecturally different 
and far more relevant to high-frequency financial data.

**Why episodic memory (pgvector)?**

Agents store past incidents in a vector store. When a new anomaly 
hits, similar past incidents are retrieved as context. 
The system gets smarter over time.

---

## Tech Stack
Streaming:      Apache Kafka + Schema Registry
AI Agents:      LangGraph + Llama 3.2 (Ollama — free, local)
Memory:         pgvector + PostgreSQL
Observability:  Prometheus + Grafana
API:            FastAPI
Infrastructure: Docker + Docker Compose
Cloud:          AWS EC2
Languages:      Python, Java

---

## Observability

StreamSentinel exposes real-time metrics via Prometheus
and visualizes them in a live Grafana dashboard.

**Metrics tracked:**

| Metric | What it measures |
|--------|-----------------|
| `streamsentinel_messages_total` | Messages processed per topic |
| `streamsentinel_anomalies_total` | Anomalies detected by type and severity |
| `streamsentinel_pipeline_duration_seconds` | Full pipeline processing time (MTTD) |
| `streamsentinel_active_anomalies` | Currently active anomalies |
| `streamsentinel_remediations_total` | Auto-remediations vs escalations |

**View live dashboard:** http://localhost:3000

---

## Anomaly Types Detected

- 🔴 **LARGE_TRANSACTION** — Suspicious transaction amount
- 🔴 **SILENT_STREAM** — No messages for 60+ seconds  
- 🟡 **RATE_DROP** — Message rate drops 70%+ suddenly
- 🟡 **SCHEMA_DRIFT** — Upstream schema change breaks consumers

---

## Blast Radius Scoring

| Score | Meaning | Action |
|-------|---------|--------|
| 🟢 LOW | Isolated impact | Auto-remediate silently |
| 🟡 MEDIUM | Analytics affected | Auto-remediate + notify team |
| 🔴 HIGH | Executive/ML systems affected | Escalate to human immediately |

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.9+
- Ollama (for local LLM — free, no API key needed)

### Run locally

```bash
# Clone the repo
git clone https://github.com/Neelam95/StreamSentinel.git
cd StreamSentinel

# Start Kafka + Prometheus + Grafana
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Pull the AI model (free, runs locally)
ollama pull llama3.2

# Start StreamSentinel
python main.py
```

### View the dashboard

Open **http://localhost:3000** in your browser.
- Username: `admin`
- Password: `streamsentinel`

---

## Sample Output
🔴🔴🔴 ANOMALY #1 — FULL PIPELINE STARTING
Step 1/4 — DiagnosisAgent diagnosing...
🧠 AI DIAGNOSIS COMPLETE
Root cause: Misconfigured payment gateway
Business impact: Regulatory exposure risk
Step 2/4 — BlastRadiusAgent scoring...
🔴 Blast Radius: HIGH
Affected: fraud-detection, accounting, compliance-reporting
Step 3/4 — RemediationAgent taking action...
🔴 HUMAN ESCALATION REQUIRED
⚠️  DO NOT AUTO-FIX — HUMAN DECISION REQUIRED
📟 ON-CALL ENGINEER PAGED
Step 4/4 — NarratorAgent writing report...
📰 INCIDENT POST-MORTEM REPORT saved to logs/
✅✅✅ ANOMALY #1 — PIPELINE COMPLETE

---

## Building in Public

I'm building StreamSentinel in public on LinkedIn.
Follow the journey: [Neelam Borse](https://www.linkedin.com/in/gauriborse/)

---

## Author

**Neelam Borse** — Backend & Distributed Systems Engineer

- 5+ years building production data pipelines
- Currently @ Capital Group — Kafka, Spark, AWS at scale
- LinkedIn: [linkedin.com/in/gauriborse](https://www.linkedin.com/in/gauriborse/)
- GitHub: [github.com/Neelam95](https://github.com/Neelam95)