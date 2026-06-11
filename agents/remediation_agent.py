import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - RemediationAgent - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RemediationAgent:
    """
    RemediationAgent - The Action Taker of StreamSentinel
    
    Takes the blast radius assessment and decides what to do:
    - LOW: fix it automatically and silently
    - MEDIUM: fix it and notify the team
    - HIGH: escalate to human immediately, don't touch anything
    
    Think of it like a surgeon who only operates
    when it's safe to do so.
    """
    
    def __init__(self):
        self.remediations = []
        self.escalations = []
        self.max_history = 100
        logger.info("RemediationAgent initialized")
    
    def remediate(self, anomaly: dict, blast_assessment: dict) -> dict:
        """
        Main method — decides and executes the right action
        based on blast radius
        """
        blast_radius = blast_assessment.get("blast_radius")
        anomaly_type = anomaly.get("type")
        topic = blast_assessment.get("topic")
        
        logger.info(
            f"🔧 RemediationAgent received anomaly: "
            f"{anomaly_type} on {topic} "
            f"[Blast Radius: {blast_radius}]"
        )
        
        if blast_radius == "LOW":
            return self._auto_remediate(anomaly, blast_assessment, notify=False)
        elif blast_radius == "MEDIUM":
            return self._auto_remediate(anomaly, blast_assessment, notify=True)
        elif blast_radius == "HIGH":
            return self._escalate_to_human(anomaly, blast_assessment)
        else:
            return self._unknown_action(anomaly, blast_assessment)
    
    def _auto_remediate(
        self, 
        anomaly: dict, 
        blast_assessment: dict,
        notify: bool
    ) -> dict:
        """
        Automatically fix the issue.
        For LOW: silently.
        For MEDIUM: with notification.
        """
        anomaly_type = anomaly.get("type")
        topic = blast_assessment.get("topic")
        blast_radius = blast_assessment.get("blast_radius")
        
        # Determine the fix based on anomaly type
        fix_applied = self._determine_fix(anomaly_type, topic)
        
        result = {
            "action": "AUTO_REMEDIATED",
            "blast_radius": blast_radius,
            "anomaly_type": anomaly_type,
            "topic": topic,
            "fix_applied": fix_applied,
            "notified_team": notify,
            "timestamp": datetime.utcnow().isoformat(),
            "resolved_by": "RemediationAgent"
        }
        
        self.remediations.append(result)
        if len(self.remediations) > self.max_history:
            self.remediations.pop(0)
        
        emoji = "🟡" if notify else "🟢"
        
        logger.info(
            f"\n{'='*55}\n"
            f"{emoji} AUTO-REMEDIATION APPLIED\n"
            f"{'='*55}\n"
            f"Anomaly: {anomaly_type}\n"
            f"Topic: {topic}\n"
            f"Blast Radius: {blast_radius}\n"
            f"Fix Applied: {fix_applied}\n"
            f"Team Notified: {notify}\n"
            f"Resolved At: {result['timestamp']}\n"
            f"{'='*55}"
        )
        
        if notify:
            self._send_notification(anomaly, fix_applied, blast_assessment)
        
        return result
    
    def _escalate_to_human(
        self, 
        anomaly: dict, 
        blast_assessment: dict
    ) -> dict:
        """
        HIGH blast radius — don't touch anything.
        Wake up a human and give them full context.
        """
        anomaly_type = anomaly.get("type")
        topic = blast_assessment.get("topic")
        affected = blast_assessment.get("affected_services", [])
        
        escalation = {
            "action": "ESCALATED_TO_HUMAN",
            "blast_radius": "HIGH",
            "anomaly_type": anomaly_type,
            "topic": topic,
            "affected_services": affected,
            "timestamp": datetime.utcnow().isoformat(),
            "escalated_by": "RemediationAgent",
            "reason": "High blast radius — autonomous action too risky"
        }
        
        self.escalations.append(escalation)
        if len(self.escalations) > self.max_history:
            self.escalations.pop(0)
        
        logger.warning(
            f"\n{'='*55}\n"
            f"🔴 HUMAN ESCALATION REQUIRED\n"
            f"{'='*55}\n"
            f"Anomaly: {anomaly_type}\n"
            f"Topic: {topic}\n"
            f"Blast Radius: HIGH\n"
            f"Affected Services: {affected}\n"
            f"Reason: Too risky to auto-remediate\n"
            f"⚠️  DO NOT AUTO-FIX — HUMAN DECISION REQUIRED\n"
            f"Escalated At: {escalation['timestamp']}\n"
            f"{'='*55}"
        )
        
        # In production this would page the on-call engineer
        # via PagerDuty, Slack, or SMS
        self._page_oncall_engineer(anomaly, blast_assessment)
        
        return escalation
    
    def _determine_fix(self, anomaly_type: str, topic: str) -> str:
        """
        Decide what fix to apply based on anomaly type.
        Think of this as the playbook for common incidents.
        """
        fixes = {
            "LARGE_TRANSACTION": 
                "Flagged transaction for manual review. Added to watchlist.",
            "SILENT_STREAM": 
                "Restarted consumer group. Checked broker connectivity.",
            "RATE_DROP": 
                "Scaled up consumer instances. Checked producer health.",
            "SCHEMA_DRIFT": 
                "Applied schema evolution. Updated consumer deserializer."
        }
        return fixes.get(anomaly_type, "Applied standard recovery procedure.")
    
    def _send_notification(
        self, 
        anomaly: dict, 
        fix_applied: str,
        blast_assessment: dict
    ):
        """
        Send a notification to the team.
        In production: Slack webhook, email, etc.
        For now: logs it clearly.
        """
        logger.info(
            f"📢 TEAM NOTIFICATION SENT\n"
            f"   Anomaly: {anomaly.get('type')}\n"
            f"   Fix: {fix_applied}\n"
            f"   Affected: {blast_assessment.get('affected_services')}\n"
            f"   Status: Resolved automatically"
        )
    
    def _page_oncall_engineer(
        self, 
        anomaly: dict,
        blast_assessment: dict
    ):
        """
        Page the on-call engineer.
        In production: PagerDuty, OpsGenie, Slack @oncall
        For now: logs the escalation clearly.
        """
        logger.warning(
            f"📟 ON-CALL ENGINEER PAGED\n"
            f"   Anomaly: {anomaly.get('type')}\n"
            f"   Topic: {blast_assessment.get('topic')}\n"
            f"   Affected Services: "
            f"{blast_assessment.get('high_criticality_services')}\n"
            f"   Action Required: Immediate investigation"
        )
    
    def _unknown_action(
        self, 
        anomaly: dict,
        blast_assessment: dict
    ) -> dict:
        """Fallback for unknown blast radius"""
        logger.error("Unknown blast radius — defaulting to human escalation")
        return self._escalate_to_human(anomaly, blast_assessment)
    
    def get_summary(self) -> dict:
        """Summary of all actions taken"""
        return {
            "total_remediations": len(self.remediations),
            "total_escalations": len(self.escalations),
            "auto_remediation_rate": (
                len(self.remediations) / 
                max(len(self.remediations) + len(self.escalations), 1) * 100
            )
        }


if __name__ == "__main__":
    from blast_radius_agent import BlastRadiusAgent
    
    blast_agent = BlastRadiusAgent()
    remediation_agent = RemediationAgent()
    
    print("\n🧪 Test 1: LOW blast radius — should auto-fix silently")
    low_anomaly = {
        "type": "RATE_DROP",
        "severity": "LOW",
        "message": "Minor rate drop detected"
    }
    low_assessment = blast_agent.calculate_blast_radius(
        "unknown-topic", "RATE_DROP"
    )
    remediation_agent.remediate(low_anomaly, low_assessment)
    
    print("\n🧪 Test 2: HIGH blast radius — should escalate to human")
    high_anomaly = {
        "type": "LARGE_TRANSACTION",
        "severity": "HIGH",
        "message": "Unusually large transaction: $750,000"
    }
    high_assessment = blast_agent.calculate_blast_radius(
        "financial-transactions", "LARGE_TRANSACTION"
    )
    remediation_agent.remediate(high_anomaly, high_assessment)
    
    print("\n📊 REMEDIATION SUMMARY:")
    summary = remediation_agent.get_summary()
    print(f"   Total auto-remediations: {summary['total_remediations']}")
    print(f"   Total escalations: {summary['total_escalations']}")
    print(f"   Auto-remediation rate: {summary['auto_remediation_rate']:.1f}%")