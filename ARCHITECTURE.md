# Architecture Decision Record — StreamSentinel

This document explains the key architectural decisions made while 
building StreamSentinel and the reasoning behind each one.

These are the decisions that distinguish StreamSentinel from 
other pipeline monitoring projects.

---

## ADR-001: Kafka-native over Airflow

**Decision:** StreamSentinel runs directly on Kafka consumer groups,
not Airflow DAGs.

**Context:**
Every existing self-healing pipeline project wraps Airflow DAGs.
Airflow is a scheduler — it runs jobs on a schedule.
Kafka consumer groups watch live streams continuously.

**Reasoning:**
Pipeline failures in high-frequency financial data don't wait for 
the next scheduled run. A silent stream at 2am needs to be caught 
in seconds, not at the next 15-minute interval.

Running directly on Kafka consumer groups means:
- Zero latency between failure and detection
- Native integration with the streaming infrastructure
- No additional scheduler dependency

**Alternatives considered:**
- Airflow DAGs — rejected: scheduled, not real-time
- Spark Streaming — rejected: too heavy for monitoring workload
- Custom polling script — rejected: not production-grade

---

## ADR-002: Deterministic Blast Radius Scoring (No AI)

**Decision:** BlastRadiusAgent uses zero LLM — pure BFS graph 
traversal over a dependency graph.

**Context:**
Every incident requires a decision: auto-fix or escalate to human?
This decision has real operational consequences.

**Reasoning:**
LLMs introduce non-determinism. The same input can produce 
different outputs on different runs. For a governance decision 
that determines whether a pipeline feeding compliance reporting 
gets auto-modified — that's unacceptable.

BFS graph traversal produces the same answer every time.
It's auditable, explainable, and fast.

The rule: if any HIGH criticality service is affected → escalate.
No exceptions. No probabilistic reasoning.

**Alternatives considered:**
- LLM-based impact scoring — rejected: non-deterministic
- Static threshold rules — rejected: doesn't model dependencies
- Human decision every time — rejected: defeats the purpose

**Quote:**
> "Guardrails warn. Gates prevent. One is an observer. 
> The other is architecture."

---

## ADR-003: Local LLM via Ollama (No Cloud API)

**Decision:** DiagnosisAgent and NarratorAgent use Llama 3.2 
running locally via Ollama.

**Context:**
The system processes financial transaction data. Sending that 
data to a cloud API creates privacy, compliance, and cost concerns.

**Reasoning:**
- Zero data leaves the machine
- No API key required
- No per-token cost
- Works offline
- Llama 3.2 via 4-bit quantization fits on an M1 MacBook Air

For structured diagnosis tasks (root cause, impact, action), 
a local 3B parameter model is sufficient.

**Alternatives considered:**
- OpenAI API — rejected: data privacy, ongoing cost
- Anthropic API — rejected: data privacy, ongoing cost
- No LLM — rejected: plain rule-based diagnosis lacks context

---

## ADR-004: Human-in-the-Loop Gate for HIGH Blast Radius

**Decision:** RemediationAgent refuses to take any action when 
blast radius is HIGH. It pages a human instead.

**Context:**
Auto-remediation is powerful but dangerous at scale. 
A wrong auto-fix on a pipeline feeding executive dashboards 
and ML models can cause more damage than the original failure.

**Reasoning:**
The system knows its own limits. When blast radius is HIGH:
- Fraud detection service affected
- Accounting service affected  
- Compliance reporting affected

No automated system should modify these without human judgment.
The agent pages the on-call engineer with full context:
anomaly type, blast radius, AI diagnosis, affected services.

The human makes the call. The system provides everything they 
need to make it fast.

**Alternatives considered:**
- Auto-fix everything — rejected: too risky for HIGH impact
- Alert and do nothing — rejected: doesn't help the engineer
- Require human approval for all actions — rejected: defeats LOW/MEDIUM automation

---

## ADR-005: Episodic Memory via pgvector

**Decision:** Every incident is stored as a vector embedding 
in PostgreSQL + pgvector. Similar past incidents are retrieved 
as context for new diagnoses.

**Context:**
A fresh system has no context. Every incident starts from scratch.
An experienced engineer has pattern recognition from hundreds 
of past incidents.

**Reasoning:**
Vector similarity search lets the system find incidents that are 
semantically similar — not just exact matches.

When a new anomaly hits:
1. Generate embedding for the new anomaly
2. Search pgvector for similar past incidents
3. Pass top-3 similar incidents as context to DiagnosisAgent
4. LLM diagnosis improves with every incident processed

The system gets smarter over time. A system that has seen 
1000 incidents diagnoses faster and more accurately than 
one seeing its first.

**Alternatives considered:**
- No memory — rejected: system never improves
- Simple key-value store — rejected: no semantic similarity
- Elasticsearch — rejected: adds operational complexity
- In-memory store — rejected: lost on restart

---

## ADR-006: Automatic Post-Mortem Generation

**Decision:** NarratorAgent writes a plain-English incident 
post-mortem automatically after every incident.

**Context:**
Post-mortems are one of the most valuable engineering practices.
They're also one of the most skipped — because writing them 
at 3am after fixing an incident is brutal.

**Reasoning:**
The system already has all the information needed for a 
post-mortem:
- What was detected (WatcherAgent)
- Why it happened (DiagnosisAgent)
- How bad it was (BlastRadiusAgent)
- What was done (RemediationAgent)

NarratorAgent combines all of this into a structured Markdown 
report saved to disk automatically.

No engineer needs to write post-mortems. No context is lost.
Every incident is documented.

**Alternatives considered:**
- Manual post-mortems — rejected: never written consistently
- Simple log aggregation — rejected: not human-readable
- Template-based reports — rejected: lacks AI-generated insights

---

## Summary

| Decision | Choice | Key Reason |
|----------|--------|------------|
| Stream processing | Kafka-native | Real-time, not scheduled |
| Impact scoring | Deterministic BFS | Governance requires certainty |
| LLM deployment | Local via Ollama | Data privacy, zero cost |
| High-impact actions | Human-in-the-loop | System knows its limits |
| Agent memory | pgvector embeddings | Semantic similarity search |
| Incident documentation | Auto-generated | Consistency, no 3am writing |