import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - DataProducer - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProducer:
    """
    DataProducer - Simulates real financial data streams
    
    Think of this as the machine that keeps sending
    letters to our post office (Kafka) automatically.
    """
    
    def __init__(self, kafka_servers: str):
        self.kafka_servers = kafka_servers
        self.producer = None
        self.messages_sent = 0
        logger.info("DataProducer initialized")

    def connect(self):
        """Connect to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info("✅ DataProducer connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            return False

    def generate_transaction(self):
        """Generate a fake but realistic financial transaction"""
        transaction_types = ["trade", "transfer", "deposit", "withdrawal"]
        currencies = ["USD", "EUR", "GBP", "JPY"]
        
        return {
            "event_id": f"EVT-{random.randint(10000, 99999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": random.choice(transaction_types),
            "amount": round(random.uniform(100, 100000), 2),
            "currency": random.choice(currencies),
            "source_account": f"ACC-{random.randint(1000, 9999)}",
            "destination_account": f"ACC-{random.randint(1000, 9999)}",
            "status": "pending"
        }

    def generate_trade_event(self):
        """Generate a fake trade event"""
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        
        return {
            "event_id": f"TRD-{random.randint(10000, 99999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": random.choice(symbols),
            "quantity": random.randint(1, 1000),
            "price": round(random.uniform(50, 500), 2),
            "trade_type": random.choice(["buy", "sell"]),
            "trader_id": f"TRD-{random.randint(100, 999)}"
        }

    def produce(self, messages_per_minute: int = 10):
        """
        Start producing messages automatically
        Default: 10 messages per minute
        """
        if not self.connect():
            return

        interval = 60 / messages_per_minute
        logger.info(f"🚀 Starting to produce {messages_per_minute} messages/minute")
        logger.info("Press Ctrl+C to stop")

        try:
            while True:
                # Send financial transaction
                transaction = self.generate_transaction()
                self.producer.send("financial-transactions", transaction)
                self.messages_sent += 1
                logger.info(
                    f"📤 Sent transaction: {transaction['event_type']} | "
                    f"Amount: {transaction['amount']} {transaction['currency']} | "
                    f"Total sent: {self.messages_sent}"
                )

                # Every 3 transactions, also send a trade event
                if self.messages_sent % 3 == 0:
                    trade = self.generate_trade_event()
                    self.producer.send("trade-events", trade)
                    logger.info(
                        f"📤 Sent trade: {trade['symbol']} | "
                        f"{trade['trade_type'].upper()} {trade['quantity']} "
                        f"@ ${trade['price']}"
                    )

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info(f"DataProducer stopped. Total messages sent: {self.messages_sent}")
        finally:
            if self.producer:
                self.producer.close()


if __name__ == "__main__":
    producer = DataProducer(kafka_servers="localhost:9092")
    producer.produce(messages_per_minute=10)