import time
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from kafka import KafkaConsumer
from agents.diagnosis_agent import DiagnosisAgent
from agents.blast_radius_agent import BlastRadiusAgent
from agents.remediation_agent import RemediationAgent
from agents.narrator_agent import NarratorAgent
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from agents.memory_agent import MemoryAgent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - StreamSentinel - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSentinel:
    def __init__(self):
        self.kafka_servers = "localhost:9092"
        self.topics = ["financial-transactions", "trade-events"]
        logger.info("🚀 Initializing StreamSentinel...")
        self.diagnosis_agent = DiagnosisAgent()
        self.blast_agent = BlastRadiusAgent()
        self.remediation_agent = RemediationAgent()
        self.narrator_agent = NarratorAgent()
        self.memory_agent = MemoryAgent()
        self.messages_total = Counter(
            'streamsentinel_messages_total',
            'Total messages processed',
            ['topic']
        )
        self.anomalies_total = Counter(
            'streamsentinel_anomalies_total',
            'Total anomalies detected',
            ['type', 'severity']
        )
        self.remediations_total = Counter(
            'streamsentinel_remediations_total',
            'Total remediations',
            ['action']
        )
        self.pipeline_duration = Histogram(
            'streamsentinel_pipeline_duration_seconds',
            'Pipeline duration'
        )
        self.active_anomalies = Gauge(
            'streamsentinel_active_anomalies',
            'Active anomalies'
        )
        self.total_messages = 0
        self.total_anomalies = 0
        self.start_time = datetime.utcnow()
        logger.info("✅ All 5 agents initialized and ready!")

    def connect_to_kafka(self):
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset='latest',
                group_id='streamsentinel-main',
                value_deserializer=lambda x: json.loads(
                    x.decode('utf-8')
                )
            )
            logger.info("✅ Connected to Kafka streams")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            return False

    def detect_anomaly(self, message_value: dict, topic: str):
        if topic == "financial-transactions":
            amount = message_value.get("amount", 0)
            if amount > 500000:
                return {
                    "type": "LARGE_TRANSACTION",
                    "severity": "MEDIUM",
                    "message": f"Unusually large transaction: {amount} "
                               f"{message_value.get('currency', 'USD')}",
                    "topic": topic,
                    "data": message_value,
                    "timestamp": datetime.utcnow().isoformat()
                }
        return None

    def handle_anomaly(self, anomaly: dict):
        self.total_anomalies += 1
        topic = anomaly.get("topic", "unknown")
        self.anomalies_total.labels(
            type=anomaly.get("type", "unknown"),
            severity=anomaly.get("severity", "unknown")
        ).inc()
        self.active_anomalies.inc()
        start = time.time()
        logger.info(
            f"\n{'🔴' * 20}\n"
            f"ANOMALY #{self.total_anomalies} — FULL PIPELINE STARTING\n"
            f"{'🔴' * 20}"
        )
        # Check memory for similar past incidents
        similar_incidents = self.memory_agent.retrieve_similar(anomaly)
        if similar_incidents:
            logger.info(
                f"🧠 Found {len(similar_incidents)} similar past incidents — "
                f"adding context to diagnosis"
            )
            anomaly = {**anomaly, "similar_incidents": similar_incidents}

        logger.info("Step 1/4 — DiagnosisAgent diagnosing...")
        diagnosis = self.diagnosis_agent.diagnose(anomaly)

        logger.info("Step 2/4 — BlastRadiusAgent scoring impact...")
        blast = self.blast_agent.calculate_blast_radius(
            topic, anomaly.get("type")
        )
        logger.info("Step 3/4 — RemediationAgent taking action...")
        remediation = self.remediation_agent.remediate(anomaly, blast)
        self.remediations_total.labels(
            action=remediation.get("action", "unknown")
        ).inc()
        logger.info("Step 4/4 — NarratorAgent writing report...")
        self.narrator_agent.narrate(
            anomaly, diagnosis, blast, remediation
        )
        # Store in long-term memory
        self.memory_agent.store_incident(
            anomaly, diagnosis, blast, remediation,
            {"narrative": "incident processed"}
        )

        duration = time.time() - start
        self.pipeline_duration.observe(duration)
        self.active_anomalies.dec()
        logger.info(
            f"\n{'✅' * 20}\n"
            f"ANOMALY #{self.total_anomalies} — PIPELINE COMPLETE\n"
            f"Duration: {duration:.2f}s\n"
            f"{'✅' * 20}"
        )

    def print_status(self):
        while True:
            time.sleep(30)
            uptime = (datetime.utcnow() - self.start_time).seconds
            logger.info(
                f"\n📊 STATUS\n"
                f"   Uptime: {uptime}s\n"
                f"   Messages: {self.total_messages}\n"
                f"   Anomalies: {self.total_anomalies}"
            )

    def run(self):
        logger.info(
            f"\n{'='*60}\n"
            f"🚀 STREAMSENTINEL STARTING\n"
            f"{'='*60}"
        )
        start_http_server(8000)
        logger.info("📊 Prometheus metrics at http://localhost:8000")

        # Start health check endpoint on port 8001
        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"OK")
                def log_message(self, format, *args):
                    pass

        health_server = HTTPServer(("", 8001), HealthHandler)
        health_thread = threading.Thread(
            target=health_server.serve_forever,
            daemon=True
        )
        health_thread.start()
        logger.info("❤️ Health check at http://localhost:8001/health")
        if not self.connect_to_kafka():
            return
        status_thread = threading.Thread(
            target=self.print_status,
            daemon=True
        )
        status_thread.start()
        logger.info("👀 Watching for anomalies... (Ctrl+C to stop)")
        try:
            for message in self.consumer:
                self.total_messages += 1
                topic = message.topic
                value = message.value
                self.messages_total.labels(topic=topic).inc()
                logger.info(
                    f"📨 Message #{self.total_messages} | Topic: {topic}"
                )
                try:
                    anomaly = self.detect_anomaly(value, topic)
                    if anomaly:
                        self.handle_anomaly(anomaly)
                except Exception as e:
                    logger.error(
                        f"❌ Failed to process message from topic {topic}: {e} "
                        f"— message skipped, pipeline continues"
                    )
        except KeyboardInterrupt:
            logger.info("⏳ Shutdown signal received — waiting for current pipeline to complete...")
            self.consumer.close()
            logger.info("✅ StreamSentinel stopped gracefully")
        finally:
            uptime = (datetime.utcnow() - self.start_time).seconds
            logger.info(
                f"\n{'='*60}\n"
                f"📊 FINAL REPORT\n"
                f"   Uptime: {uptime}s\n"
                f"   Messages: {self.total_messages}\n"
                f"   Anomalies: {self.total_anomalies}\n"
                f"{'='*60}"
            )

if __name__ == "__main__":
    sentinel = StreamSentinel()
    sentinel.run()