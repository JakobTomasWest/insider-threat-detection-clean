import sys
import os
import json

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ui import app

def verify_phase2():
    print("Verifying Phase 2...")
    
    # Mock alerts
    alerts = [
        {
            "day": "2025-01-01",
            "forecast_score": 0.8, # Forecast spike
            "rule_hits": [],
            "rule_timeline": []
        },
        {
            "day": "2025-01-05",
            "rule_hits": ["s1_chain"],
            "rule_timeline": [
                {"day": "2025-01-02", "kind": "after_hours", "message": "AH event"},
                {"day": "2025-01-02", "kind": "usb", "message": "USB event"},
                {"day": "2025-01-05", "kind": "leak_site", "message": "Leak event"}
            ]
        },
        # Duplicate timeline event in later alert (should be deduped)
        {
            "day": "2025-01-06",
            "rule_hits": ["s1_chain"],
            "rule_timeline": [
                {"day": "2025-01-05", "kind": "leak_site", "message": "Leak event"}, # Duplicate
                {"day": "2025-01-06", "kind": "usb", "message": "USB event 2"}
            ]
        }
    ]
    
    notes = app._build_analyst_notes_for_user("user1", alerts)
    
    print(f"Generated {len(notes)} notes.")
    for n in notes:
        print(f"  - {n['day']} [{n['kind']}]: {n['message']}")
        
    # Check for high-level notes
    kinds = [n["kind"] for n in notes]
    if "forecast_spike" not in kinds:
        print("FAILURE: Missing forecast_spike.")
        sys.exit(1)
    if "full_chain" not in kinds:
        print("FAILURE: Missing full_chain.")
        sys.exit(1)
        
    # Check for rule events
    if "rule_after_hours" not in kinds:
        print("FAILURE: Missing rule_after_hours.")
        sys.exit(1)
    if "rule_usb" not in kinds:
        print("FAILURE: Missing rule_usb.")
        sys.exit(1)
    if "rule_leak_site" not in kinds:
        print("FAILURE: Missing rule_leak_site.")
        sys.exit(1)
        
    # Check for duplicates
    leak_events = [n for n in notes if n["kind"] == "rule_leak_site"]
    if len(leak_events) != 1:
        print(f"FAILURE: Expected 1 rule_leak_site event, found {len(leak_events)}.")
        sys.exit(1)
        
    # Check sorting
    days = [n["day"] for n in notes]
    if days != sorted(days):
        print("FAILURE: Notes not sorted by day.")
        sys.exit(1)
        
    print("SUCCESS: Phase 2 verification passed.")

if __name__ == "__main__":
    verify_phase2()
