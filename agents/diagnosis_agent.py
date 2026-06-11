import json
import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - DiagnosisAgent - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiagnosisAgent:
    """
    DiagnosisAgent - The brain of StreamSentinel
    
    Takes an anomaly detected by WatcherAgent and asks
    the AI to explain what happened and what to do next.
    
    Like a senior engineer who looks at an alert and says
    "here's what's wrong and here's how to fix it."
    """
    
    def __init__(self):
        self.ollama_url = "http://localhost:11434/api/generate"
        self.model = "llama3.2"
        self.diagnoses = []
        logger.info("DiagnosisAgent initialized with Llama 3.2")
    
    def build_prompt(self, anomaly: dict) -> str:
        """
        Build a clear prompt for the AI.
        
        Think of this like writing a message to a senior engineer
        explaining what just happened and asking for their opinion.
        """
        return f"""You are a senior data engineer analyzing a real-time 
financial data pipeline anomaly.

ANOMALY DETECTED:
- Type: {anomaly.get('type', 'UNKNOWN')}
- Severity: {anomaly.get('severity', 'UNKNOWN')}
- Details: {anomaly.get('message', 'No details')}
- Time: {anomaly.get('timestamp', datetime.utcnow().isoformat())}
- Amount Band: {'HIGH' if anomaly.get('data', {}).get('amount', 0) > 100000 else 'MEDIUM' if anomaly.get('data', {}).get('amount', 0) > 10000 else 'LOW'}
- Currency: {anomaly.get('data', {}).get('currency', 'UNKNOWN')}

Please provide:
1. ROOT CAUSE: What most likely caused this anomaly?
2. BUSINESS IMPACT: How does this affect the business right now?
3. IMMEDIATE ACTION: What should the on-call engineer do in the next 5 minutes?
4. PREVENTION: How can we prevent this from happening again?

Keep your response concise and practical. Think like an engineer 
who has seen this before, not like a textbook."""

    def ask_ai(self, prompt: str) -> str:
        """Send the anomaly to our local AI and get a diagnosis"""
        def _call_ollama():
            response = requests.post(
                self.ollama_url,
                json={
                "model": self.model,
                "prompt": prompt,
                "stream": False
                },
                timeout=60
            )
            if response.status_code == 200:
                result = response.json()
                return result.get("response", "No response from AI")
            else:
                return f"AI unavailable: {response.status_code}"

        try:
            logger.info("🧠 Asking AI to diagnose the anomaly...")
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_call_ollama)
                return future.result(timeout=65)
        except TimeoutError:
            logger.error("Ollama diagnosis timed out")
            return "AI diagnosis timed out"
        except Exception as e:
            logger.error(f"Failed to reach AI: {e}")
            return "AI diagnosis unavailable"
    
    def diagnose(self, anomaly: dict) -> dict:
        """
        Main method - takes an anomaly and returns a full diagnosis
        
        This is like handing an incident report to a senior engineer
        and getting back a full post-mortem.
        """
        logger.info(f"🔍 Diagnosing anomaly: {anomaly.get('type')}")
        
        # Build the prompt
        prompt = self.build_prompt(anomaly)
        
        # Ask the AI
        ai_response = self.ask_ai(prompt)
        
        # Package the diagnosis
        diagnosis = {
            "anomaly_type": anomaly.get("type"),
            "severity": anomaly.get("severity"),
            "timestamp": datetime.utcnow().isoformat(),
            "anomaly_type": anomaly.get("type"),
            "anomaly_severity": anomaly.get("severity"),
            "ai_diagnosis": ai_response,
            "diagnosed_by": "DiagnosisAgent-Llama3.2"
        }
        
        self.diagnoses.append(diagnosis)
        
        # Print it nicely
        logger.info(
            f"\n{'='*60}\n"
            f"🧠 AI DIAGNOSIS COMPLETE\n"
            f"{'='*60}\n"
            f"Anomaly: {anomaly.get('type')}\n"
            f"Severity: {anomaly.get('severity')}\n"
            f"{'='*60}\n"
            f"{ai_response}\n"
            f"{'='*60}"
        )
        
        return diagnosis


if __name__ == "__main__":
    # Test the DiagnosisAgent with a fake anomaly
    agent = DiagnosisAgent()
    
    # Simulate the anomaly WatcherAgent would send
    test_anomaly = {
        "type": "LARGE_TRANSACTION",
        "severity": "MEDIUM",
        "message": "Unusually large transaction detected: 750000 USD",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "amount": 750000,
            "currency": "USD",
            "source_account": "ACC-0001",
            "destination_account": "ACC-9999",
            "event_type": "transfer"
        }
    }
    
    print("Testing DiagnosisAgent...")
    diagnosis = agent.diagnose(test_anomaly)
    print("\nDiagnosis complete!")