import time
import json
import logging
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer
from diagnosis_agent import DiagnosisAgent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - WatcherAgent - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    """
    The brain that decides if something looks wrong.
    
    Think of it like a bank fraud detector —
    it learns what's normal and flags what isn't.
    """
    
    def __init__(self):
        # Sliding window — tracks last 60 seconds of message timestamps
        self.message_timestamps = deque()
        self.window_seconds = 60
        self.min_expected_rate = 1  # at least 1 message per minute
        self.max_amount = 500000    # flag anything over $500K
        self.anomalies = []
    
    def record_message(self, timestamp: float):
        """Record that a message arrived right now"""
        self.message_timestamps.append(timestamp)
        # Clean up old timestamps outside our window
        cutoff = timestamp - self.window_seconds
        while self.message_timestamps and self.message_timestamps[0] < cutoff:
            self.message_timestamps.popleft()
    
    def get_current_rate(self) -> float:
        """How many messages per minute right now?"""
        return len(self.message_timestamps)
    
    def check_message_rate(self):
        """
        Is the message rate suspiciously low?
        Like a post office that suddenly stops getting mail.
        """
        rate = self.get_current_rate()
        if rate == 0:
            return {
                "type": "SILENT_STREAM",
                "severity": "HIGH",
                "message": "No messages received in the last 60 seconds",
                "current_rate": rate,
                "expected_min": self.min_expected_rate
            }
        return None
    
    def check_transaction_amount(self, amount: float, currency: str):
        """
        Is this transaction amount suspiciously large?
        Like a $50 million wire transfer at 3am.
        """
        if amount > self.max_amount:
            return {
                "type": "LARGE_TRANSACTION",
                "severity": "MEDIUM",
                "message": f"Unusually large transaction detected: {amount} {currency}",
                "amount": amount,
                "threshold": self.max_amount
            }
        return None
    
    def check_rate_drop(self, previous_rate: float, current_rate: float):
        """
        Did the message rate suddenly drop?
        Like a busy highway suddenly going empty.
        """
        if previous_rate > 5 and current_rate < previous_rate * 0.3:
            drop_percent = ((previous_rate - current_rate) / previous_rate) * 100
            return {
                "type": "RATE_DROP",
                "severity": "HIGH",
                "message": f"Message rate dropped by {drop_percent:.1f}%",
                "previous_rate": previous_rate,
                "current_rate": current_rate
            }
        return None


class WatcherAgent:
    """
    WatcherAgent - The eyes of StreamSentinel
    Now with anomaly detection! 🚨
    """
    
    def __init__(self, kafka_servers: str, topics: list):
        self.kafka_servers = kafka_servers
        self.topics = topics
        self.message_counts = {}
        self.anomalies_detected = 0
        self.detector = AnomalyDetector()
        self.previous_rate = 0
        self.diagnosis_agent = DiagnosisAgent()
        logger.info(f"WatcherAgent initialized - watching topics: {topics}")
    
    def connect_to_kafka(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset='latest',
                group_id='streamsentinel-watcher',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("✅ Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def handle_anomaly(self, anomaly: dict, topic: str):
        """What to do when we find something suspicious"""
        self.anomalies_detected += 1
    
        severity_emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"
    
        logger.warning(
            f"\n{'='*50}\n"
            f"{severity_emoji} ANOMALY #{self.anomalies_detected} DETECTED\n"
            f"Type: {anomaly['type']}\n"
            f"Severity: {anomaly['severity']}\n"
            f"Topic: {topic}\n"
            f"Details: {anomaly['message']}\n"
            f"Time: {datetime.utcnow().isoformat()}\n"
            f"{'='*50}"
        )
    
        # Automatically send to DiagnosisAgent
        logger.info("🧠 Sending to DiagnosisAgent for analysis...")
        anomaly["timestamp"] = datetime.utcnow().isoformat()
        anomaly["topic"] = topic
        self.diagnosis_agent.diagnose(anomaly)
    
    def watch(self):
        """Main watching loop"""
        logger.info("👀 WatcherAgent starting watch...")
        
        if not self.connect_to_kafka():
            return
        
        start_time = time.time()
        message_count = 0
        last_rate_check = time.time()
        
        logger.info("Watching for messages and anomalies... (Press Ctrl+C to stop)")
        
        try:
            for message in self.consumer:
                now = time.time()
                message_count += 1
                topic = message.topic
                
                # Track per topic
                if topic not in self.message_counts:
                    self.message_counts[topic] = 0
                self.message_counts[topic] += 1
                
                # Record this message in our detector
                self.detector.record_message(now)
                
                # Log the message
                logger.info(
                    f"📨 Message received - "
                    f"Topic: {topic} | "
                    f"Total: {self.message_counts[topic]} | "
                    f"Rate: {self.detector.get_current_rate()}/min"
                )
                
                # Check for anomalies in the message content
                if topic == "financial-transactions":
                    data = message.value
                    if isinstance(data, dict) and "amount" in data:
                        anomaly = self.detector.check_transaction_amount(
                            data.get("amount", 0),
                            data.get("currency", "USD")
                        )
                        if anomaly:
                            self.handle_anomaly(anomaly, topic)
                
                # Every 10 seconds check the message rate
                if now - last_rate_check >= 10:
                    current_rate = self.detector.get_current_rate()
                    
                    # Check for rate drop
                    rate_anomaly = self.detector.check_rate_drop(
                        self.previous_rate, 
                        current_rate
                    )
                    if rate_anomaly:
                        self.handle_anomaly(rate_anomaly, "all-topics")
                    
                    self.previous_rate = current_rate
                    last_rate_check = now
                        
        except KeyboardInterrupt:
            logger.info("WatcherAgent stopped by user")
        finally:
            elapsed = time.time() - start_time
            logger.info(f"\n{'='*50}")
            logger.info(f"📊 SESSION SUMMARY")
            logger.info(f"   Total messages seen: {message_count}")
            logger.info(f"   Anomalies detected: {self.anomalies_detected}")
            logger.info(f"   Time watching: {elapsed:.1f} seconds")
            logger.info(f"{'='*50}")


if __name__ == "__main__":
    watcher = WatcherAgent(
        kafka_servers="localhost:9092",
        topics=["financial-transactions", "trade-events"]
    )
    watcher.watch()