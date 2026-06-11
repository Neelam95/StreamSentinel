import json
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - BlastRadiusAgent - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BlastRadiusAgent:
    """
    BlastRadiusAgent - The Judge of StreamSentinel
    
    Decides how far an anomaly's impact spreads.
    This is deliberately NOT using AI — governance
    decisions must be deterministic and auditable.
    
    Think of it like a circuit breaker — rules based,
    fast, and predictable.
    """

    
    def __init__(self):
        self.assessments = []
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "config",
            "service_graph.json"
        )
        with open(config_path) as f:
            config = json.load(f)
        self.DEPENDENCY_GRAPH = config["dependency_graph"]
        self.SERVICE_CRITICALITY = config["service_criticality"]
        logger.info("BlastRadiusAgent initialized")
        logger.info("⚡ Running in DETERMINISTIC mode — no AI for governance decisions")
    
    def get_affected_services(self, topic: str) -> list:
        """
        Find all services that depend on this topic.
        Like finding everyone in the building who will
        be affected if this pipeline goes down.
        """
        return self.DEPENDENCY_GRAPH.get(topic, [])
    
    def calculate_blast_radius(self, topic: str, anomaly_type: str) -> dict:
        """
        Score the blast radius of an anomaly.
        
        Rules:
        - Any HIGH criticality service affected → blast radius is HIGH
        - 3+ MEDIUM services affected → blast radius is MEDIUM  
        - Everything else → blast radius is LOW
        """
        affected_services = self.get_affected_services(topic)
        
        if not affected_services:
            return self._build_assessment("LOW", [], topic, anomaly_type)
        
        # Check criticality of affected services
        high_criticality = [
            s for s in affected_services 
            if self.SERVICE_CRITICALITY.get(s) == "HIGH"
        ]
        
        medium_criticality = [
            s for s in affected_services
            if self.SERVICE_CRITICALITY.get(s) == "MEDIUM"
        ]
        
        # Determine blast radius
        if len(high_criticality) >= 2:
            radius = "HIGH"
        elif len(high_criticality) == 1:
            radius = "HIGH"
        elif len(medium_criticality) >= 3:
            radius = "MEDIUM"
        else:
            radius = "LOW"
        
        return self._build_assessment(
            radius, 
            affected_services, 
            topic, 
            anomaly_type,
            high_criticality,
            medium_criticality
        )
    
    def _build_assessment(
        self, 
        radius: str, 
        affected_services: list,
        topic: str,
        anomaly_type: str,
        high_critical: list = [],
        medium_critical: list = []
    ) -> dict:
        """Build a clean assessment report"""
        
        radius_emoji = {
            "HIGH": "🔴",
            "MEDIUM": "🟡", 
            "LOW": "🟢"
        }.get(radius, "⚪")
        
        # Autonomous action based on blast radius
        autonomous_action = {
            "HIGH": "ESCALATE_TO_HUMAN",
            "MEDIUM": "AUTO_REMEDIATE_WITH_NOTIFICATION",
            "LOW": "AUTO_REMEDIATE_SILENTLY"
        }.get(radius)
        
        assessment = {
            "blast_radius": radius,
            "topic": topic,
            "anomaly_type": anomaly_type,
            "affected_services": affected_services,
            "high_criticality_services": high_critical,
            "medium_criticality_services": medium_critical,
            "autonomous_action": autonomous_action,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.assessments.append(assessment)
        
        logger.info(
            f"\n{'='*55}\n"
            f"{radius_emoji} BLAST RADIUS ASSESSMENT\n"
            f"{'='*55}\n"
            f"Topic: {topic}\n"
            f"Anomaly: {anomaly_type}\n"
            f"Blast Radius: {radius}\n"
            f"Affected Services: {len(affected_services)}\n"
            f"High Criticality: {high_critical}\n"
            f"Medium Criticality: {medium_critical}\n"
            f"Action: {autonomous_action}\n"
            f"{'='*55}"
        )
        
        return assessment


if __name__ == "__main__":
    agent = BlastRadiusAgent()
    
    print("\n🧪 Test 1: Large transaction on financial-transactions topic")
    assessment1 = agent.calculate_blast_radius(
        topic="financial-transactions",
        anomaly_type="LARGE_TRANSACTION"
    )
    
    print("\n🧪 Test 2: Silent stream on trade-events topic")
    assessment2 = agent.calculate_blast_radius(
        topic="trade-events",
        anomaly_type="SILENT_STREAM"
    )
    
    print("\n🧪 Test 3: Unknown topic")
    assessment3 = agent.calculate_blast_radius(
        topic="unknown-topic",
        anomaly_type="RATE_DROP"
    )
    
    print(f"\n✅ Total assessments made: {len(agent.assessments)}")