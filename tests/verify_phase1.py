import pandas as pd
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.detector import rules

def verify_phase1():
    print("Verifying Phase 1...")

    # Create fake data
    days = pd.date_range(start="2025-01-01", periods=7, freq="D")
    data = {
        "day": days,
        "user_key": ["user1"] * 7,
        # AH signal cols
        "logon_after_hours_rate": [0.0, 0.6, 0.0, 0.6, 0.0, 0.0, 0.0], # Days 1 and 3 have AH
        "logon_n_logon": [10, 10, 10, 10, 10, 10, 10],
        # USB
        "device_n_usb_connects": [0, 5, 0, 5, 0, 0, 0], # Days 1 and 3 have USB (same day as AH)
        # Wikileaks
        "http_n_wikileaks": [0, 0, 0, 0, 0, 0, 1], # Day 6 (last day) has WL
        # Novelty
        "ah_novel": [0, 1, 0, 1, 0, 0, 0],
        "usb_novel": [0, 1, 0, 1, 0, 0, 0],
    }
    
    df = pd.DataFrame(data)
    
    # Ensure string day for check
    day_to_check = str(days[-1].date())
    
    alerts = rules.check(df, day=day_to_check, user_key="user1")
    
    if not alerts:
        print("FAILURE: No alerts returned.")
        sys.exit(1)
        
    alert = alerts[0]
    print(f"Alert reason: {alert['reason']}")
    
    if "rule_timeline" not in alert:
        print("FAILURE: rule_timeline not found in alert.")
        sys.exit(1)
        
    timeline = alert["rule_timeline"]
    print(f"Timeline entries: {len(timeline)}")
    
    if len(timeline) == 0:
        print("FAILURE: rule_timeline is empty.")
        sys.exit(1)
        
    for entry in timeline:
        print(f"  - {entry}")
        if not all(k in entry for k in ["day", "kind", "message"]):
             print(f"FAILURE: Entry missing keys: {entry}")
             sys.exit(1)

    # Check for specific expected events
    # Day 1 (index 1): 2025-01-02. AH and USB.
    # Day 3 (index 3): 2025-01-04. AH and USB.
    # Day 6 (index 6): 2025-01-07. Leak site.
    
    kinds = [e["kind"] for e in timeline]
    if "after_hours" not in kinds:
        print("FAILURE: Missing after_hours event.")
        sys.exit(1)
    if "usb" not in kinds:
        print("FAILURE: Missing usb event.")
        sys.exit(1)
    if "leak_site" not in kinds:
        print("FAILURE: Missing leak_site event.")
        sys.exit(1)
        
    print("SUCCESS: Phase 1 verification passed.")

if __name__ == "__main__":
    verify_phase1()
