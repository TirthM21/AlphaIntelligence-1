
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.ai.ai_agent import AIAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_ai():
    print("\n=== AI Agent Connection Test ===")
    
    # Initialize agent
    ai = AIAgent()
    
    if not ai.api_key:
        print("❌ Error: No API key found in .env (FREE_LLM_API_KEY)")
        return

    print(f"Model: {ai.model}")
    print(f"Base URL: {ai.base_url}")
    print("Testing connection with a simple prompt...")

    # Test case 1: Simple commentary
    ticker = "RELIANCE"
    test_data = {
        "price": 2500.50,
        "technical_score": 85.5,
        "fundamentals": {
            "revenue_growth_yoy": 15.2,
            "net_margin": 12.5,
            "debt_to_equity": 0.4
        }
    }
    
    print(f"\n[Test 1] Generating commentary for {ticker}...")
    commentary = ai.generate_commentary(ticker, test_data)
    print(f"Result:\n{commentary}")

    # Test case 2: Question of the Day
    print(f"\n[Test 2] Generating Question of the Day...")
    qotd = ai.generate_qotd()
    print(f"Question: {qotd.get('question')}")
    print(f"Answer: {qotd.get('answer')}")
    print(f"Insight: {qotd.get('insight')}")

if __name__ == "__main__":
    test_ai()
