import time
import json
import logging
import threading
from datetime import datetime
from kafka import KafkaConsumer
from agents.diagnosis_agent import DiagnosisAgent
from agents.blast_radius_agent import BlastRadiusAgent
from agents.remediation_agent import RemediationAgent
from agents.narrator_agent import NarratorAgent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - StreamSentinel - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSentinel:
    """
    StreamSentinel — The Complete Agentic Pipeline Intelligence System
    
    Orchestrates all 5 agents:
    1. WatcherAgent — detects anomalies in real time
    2. DiagnosisAgent — AI explains what happened
    3. BlastRadiusAgent — scores the impact
    4. RemediationAgent — fixes or escalates
    5. NarratorAgent — writes the incident report
    
    One command. Fully autonomous. Zero humans needed.
    """
    
    def __init__(self):
        self.kafka_servers = "localhost:9092"
        self.topics = ["financial-transactions", "trade-events"]
        
        # Initialize all 5 agents
        logger.info("🚀 Initializing StreamSentinel...")
        self.diagnosis_agent = DiagnosisAgent()
        self.blast_agent = BlastRadiusAgent()
        self.remediation_agent = RemediationAgent()
        self.narrator_agent = NarratorAgent()
        
        # Stats
        self.total_messages = 0
        self.total_anomalies = 0
        self.start_time = datetime.utcnow()
        
        logger.info("✅ All 5 agents initialized and ready!")
    
    def connect_to_kafka(self):
        """Connect to Kafka streams"""
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
        """
        Simple anomaly detection — same logic as WatcherAgent
        Returns anomaly dict or None
        """
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
        """
        The full pipeline — all 5 agents working together:
        Detect → Diagnose → Score → Remediate → Narrate
        """
        self.total_anomalies += 1
        topic = anomaly.get("topic", "unknown")
        
        logger.info(
            f"\n{'🔴' * 20}\n"
            f"ANOMALY #{self.total_anomalies} — FULL PIPELINE STARTING\n"
            f"{'🔴' * 20}"
        )
        
        # Step 1 — DiagnosisAgent: What happened?
        logger.info("Step 1/4 — DiagnosisAgent diagnosing...")
        diagnosis = self.diagnosis_agent.diagnose(anomaly)
        
        # Step 2 — BlastRadiusAgent: How bad is it?
        logger.info("Step 2/4 — BlastRadiusAgent scoring impact...")
        blast = self.blast_agent.calculate_blast_radius(
            topic, anomaly.get("type")
        )
        
        # Step 3 — RemediationAgent: What do we do?
        logger.info("Step 3/4 — RemediationAgent taking action...")
        remediation = self.remediation_agent.remediate(anomaly, blast)
        
        # Step 4 — NarratorAgent: Write the report
        logger.info("Step 4/4 — NarratorAgent writing report...")
        self.narrator_agent.narrate(
            anomaly, diagnosis, blast, remediation
        )
        
        logger.info(
            f"\n{'✅' * 20}\n"
            f"ANOMALY #{self.total_anomalies} — PIPELINE COMPLETE\n"
            f"{'✅' * 20}"
        )
    
    def print_status(self):
        """Print system status every 30 seconds"""
        while True:
            time.sleep(30)
            uptime = (datetime.utcnow() - self.start_time).seconds
            logger.info(
                f"\n📊 STREAMSENTINEL STATUS\n"
                f"   Uptime: {uptime} seconds\n"
                f"   Messages processed: {self.total_messages}\n"
                f"   Anomalies detected: {self.total_anomalies}\n"
                f"   Auto-remediations: "
                f"{len(self.remediation_agent.remediations)}\n"
                f"   Escalations: "
                f"{len(self.remediation_agent.escalations)}\n"
                f"   Reports generated: "
                f"{len(self.narrator_agent.reports)}"
            )
    
    def run(self):
        """
        Main run loop — starts StreamSentinel
        This is the one command that runs everything.
        """
        logger.info(
            f"\n{'='*60}\n"
            f"🚀 STREAMSENTINEL STARTING\n"
            f"   Watching topics: {self.topics}\n"
            f"   Agents: 5 initialized\n"
            f"   Mode: FULLY AUTONOMOUS\n"
            f"{'='*60}"
        )
        
        if not self.connect_to_kafka():
            return
        
        # Start status printer in background
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
                
                logger.info(
                    f"📨 Message #{self.total_messages} | "
                    f"Topic: {topic}"
                )
                
                # Check for anomalies
                anomaly = self.detect_anomaly(value, topic)
                if anomaly:
                    # Run the full 4-agent pipeline
                    self.handle_anomaly(anomaly)
                    
        except KeyboardInterrupt:
            logger.info("StreamSentinel stopped by user")
        finally:
            uptime = (datetime.utcnow() - self.start_time).seconds
            logger.info(
                f"\n{'='*60}\n"
                f"📊 FINAL REPORT\n"
                f"   Total uptime: {uptime} seconds\n"
                f"   Messages processed: {self.total_messages}\n"
                f"   Anomalies detected: {self.total_anomalies}\n"
                f"   Reports generated: "
                f"{len(self.narrator_agent.reports)}\n"
                f"{'='*60}"
            )


if __name__ == "__main__":
    sentinel = StreamSentinel()
    sentinel.run()