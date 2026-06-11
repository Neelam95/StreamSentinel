import os
import json
import logging
from datetime import datetime
from typing import Optional
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from sqlalchemy import create_engine, text
from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, String, DateTime, Text, Integer
from sqlalchemy.orm import declarative_base, Session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - MemoryAgent - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class IncidentMemory(Base):
    """
    Stores past incidents with their vector embeddings.
    Think of this as the agent's long-term memory.
    """
    __tablename__ = "incident_memories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    anomaly_type = Column(String(100))
    topic = Column(String(200))
    blast_radius = Column(String(20))
    diagnosis = Column(Text)
    remediation_action = Column(String(100))
    narrative = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    embedding = Column(Vector(3072))


class MemoryAgent:
    """
    MemoryAgent — The Long-Term Memory of StreamSentinel

    Stores every incident as a vector embedding in pgvector.
    When a new anomaly hits, retrieves similar past incidents
    as context for the DiagnosisAgent.

    Think of it like a doctor who remembers all past patients.
    The more incidents it sees, the smarter it gets.
    """

    def __init__(self):
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://streamsentinel:streamsentinel@localhost:5432/streamsentinel"
        )
        self.ollama_url = "http://localhost:11434/api/embeddings"
        self.model = "llama3.2"
        self.engine = None
        self.connected = False
        self._connect()

    def _connect(self):
        """Connect to PostgreSQL and create tables"""
        try:
            self.engine = create_engine(self.db_url)

            # Enable pgvector extension
            with self.engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                conn.commit()

            # Create tables
            Base.metadata.create_all(self.engine)
            self.connected = True
            logger.info("✅ MemoryAgent connected to PostgreSQL + pgvector")

        except Exception as e:
            logger.error(f"❌ MemoryAgent connection failed: {e}")
            self.connected = False

    def get_embedding(self, text_content: str) -> Optional[list]:
        """
        Convert text to a vector embedding using Llama 3.2.
        Runs in a separate thread to avoid blocking the main pipeline.
        """
        def _call_ollama():
            response = requests.post(
                self.ollama_url,
                json={
                    "model": self.model,
                    "prompt": text_content
                },
                timeout=30
            )
            if response.status_code == 200:
                return response.json().get("embedding")
            return None

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_call_ollama)
                return future.result(timeout=35)
        except TimeoutError:
            logger.error("Embedding generation timed out")
            return None
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return None

    def store_incident(
        self,
        anomaly: dict,
        diagnosis: dict,
        blast_assessment: dict,
        remediation: dict,
        narrative: dict
    ):
        """
        Store a complete incident in long-term memory.
        Called after every incident completes.
        """
        if not self.connected:
            logger.warning("MemoryAgent not connected — skipping storage")
            return

        try:
            # Build a text summary of the incident for embedding
            incident_text = (
                f"Anomaly: {anomaly.get('type')} "
                f"on topic {anomaly.get('topic')}. "
                f"Blast radius: {blast_assessment.get('blast_radius')}. "
                f"Diagnosis: {diagnosis.get('ai_diagnosis', '')[:200]}. "
                f"Action taken: {remediation.get('action')}."
            )

            # Get vector embedding
            embedding = self.get_embedding(incident_text)

            if embedding is None:
                logger.warning("Could not generate embedding — skipping storage")
                return

            if len(embedding) != 3072:
                logger.error(
                    f"Unexpected embedding dimension: {len(embedding)} — "
                    f"expected 3072. Skipping storage."
                )
                return

            # Store in database
            with Session(self.engine) as session:
                memory = IncidentMemory(
                    anomaly_type=anomaly.get("type"),
                    topic=anomaly.get("topic"),
                    blast_radius=blast_assessment.get("blast_radius"),
                    diagnosis=diagnosis.get("ai_diagnosis", "")[:1000],
                    remediation_action=remediation.get("action"),
                    narrative=narrative.get("narrative", "")[:1000],
                    embedding=embedding
                )
                session.add(memory)
                session.commit()

            logger.info(
                f"🧠 Incident stored in long-term memory: "
                f"{anomaly.get('type')} — "
                f"{blast_assessment.get('blast_radius')}"
            )

        except Exception as e:
            logger.error(f"Failed to store incident: {e}")

    def retrieve_similar(
        self,
        anomaly: dict,
        limit: int = 3
    ) -> list:
        """
        Find similar past incidents using vector similarity search.
        Returns the most relevant past incidents as context.

        This is what makes the system smarter over time.
        """
        if not self.connected:
            return []

        try:
            # Build query text
            query_text = (
                f"Anomaly: {anomaly.get('type')} "
                f"on topic {anomaly.get('topic')}. "
                f"Message: {anomaly.get('message', '')}"
            )

            # Get embedding for the query
            query_embedding = self.get_embedding(query_text)

            if query_embedding is None:
                return []

            # Search for similar incidents using cosine similarity
            with Session(self.engine) as session:
                results = session.execute(
                    text("""
                        SELECT
                            anomaly_type,
                            topic,
                            blast_radius,
                            diagnosis,
                            remediation_action,
                            timestamp,
                            1 - (embedding <=> :embedding) as similarity
                        FROM incident_memories
                        WHERE 1 - (embedding <=> :embedding) > 0.3
                        ORDER BY embedding <=> :embedding
                        LIMIT :limit
                    """),
                    {
                        "embedding": str(query_embedding),
                        "limit": limit
                    }
                ).fetchall()

            if not results:
                logger.info("No similar past incidents found")
                return []

            similar = []
            for row in results:
                similar.append({
                    "anomaly_type": row.anomaly_type,
                    "topic": row.topic,
                    "blast_radius": row.blast_radius,
                    "diagnosis": row.diagnosis,
                    "remediation_action": row.remediation_action,
                    "timestamp": str(row.timestamp),
                    "similarity": round(row.similarity, 3)
                })

            logger.info(
                f"🔍 Found {len(similar)} similar past incidents "
                f"(top similarity: {similar[0]['similarity']})"
            )

            return similar

        except Exception as e:
            logger.error(f"Retrieval failed: {e}")
            return []

    def get_memory_stats(self) -> dict:
        """How many incidents has the system learned from?"""
        if not self.connected:
            return {"total_incidents": 0}

        try:
            with Session(self.engine) as session:
                count = session.execute(
                    text("SELECT COUNT(*) FROM incident_memories")
                ).scalar()
            return {"total_incidents": count}
        except Exception as e:
            logger.error(f"Stats failed: {e}")
            return {"total_incidents": 0}


if __name__ == "__main__":
    print("Testing MemoryAgent...")
    agent = MemoryAgent()

    if agent.connected:
        stats = agent.get_memory_stats()
        print(f"✅ Connected! Total incidents in memory: {stats['total_incidents']}")
    else:
        print("❌ Connection failed")