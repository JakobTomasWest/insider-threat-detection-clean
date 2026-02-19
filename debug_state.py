
import asyncio
import sys
import os
from pathlib import Path

# Add src to sys.path
sys.path.append(os.path.abspath("src"))

from ui.app import app, STATE, load_data, get_users, get_user_alerts

async def run_diagnosis():
    print("Loading data...")
    load_data()
    
    print(f"State loaded. Users: {len(STATE.alerts_by_user)}")
    print(f"Current day: {STATE.current_day}")
    print(f"Latest day: {STATE.latest_day}")
    
    # Simulate a day where we expect some alerts
    # Let's pick a day in the middle of the timeline
    mid_day = STATE.latest_day # Start with latest day to see everything
    STATE.current_day = mid_day
    print(f"Set current_day to {STATE.current_day}")
    
    # 1. Check Hero Panel (get_users)
    print("\n--- Checking Hero Panel (get_users) ---")
    hero_users = await get_users(tab="all")
    print(f"Hero returned {len(hero_users)} users.")
    
    hero_user_keys = {u["user_key"] for u in hero_users}
    
    # 2. Check Detail Endpoint (get_user_alerts) for each Hero user
    print("\n--- Checking Detail Endpoint (get_user_alerts) ---")
    contradictions = []
    
    for u in hero_users:
        uk = u["user_key"]
        detail = await get_user_alerts(uk)
        alerts = detail["alerts"]
        
        if not alerts:
            contradictions.append(f"User {uk} is in Hero but has 0 alerts in Detail.")
        else:
            # Check if alerts are actually visible
            visible = [a for a in alerts if a["day"] <= STATE.current_day]
            if not visible:
                 contradictions.append(f"User {uk} has alerts but none visible <= {STATE.current_day} (Logic error in get_user_alerts?)")
            
            # Check for zeroed out scores in aggregation
            # Just sampling the first alert
            first = alerts[0]
            # print(f"User {uk} first alert: {first}")

    if contradictions:
        print("\n!!! CONTRADICTIONS FOUND !!!")
        for c in contradictions:
            print(c)
    else:
        print("\nNo Hero/Detail contradictions found at latest_day.")

    # 3. Check RiskMeta "Future Leak"
    print("\n--- Checking RiskMeta Future Leak ---")
    # Pick a user and set current_day to BEFORE their first alert
    if hero_users:
        test_user = hero_users[0]["user_key"]
        all_alerts = STATE.alerts_by_user[test_user]
        if all_alerts:
            sorted_alerts = sorted(all_alerts, key=lambda a: a["day"])
            first_day = sorted_alerts[0]["day"]
            
            # Set time to before first day
            STATE.current_day = "2000-01-01" 
            print(f"Time travel to {STATE.current_day} (Before {first_day})")
            
            # Hero should NOT show user
            hero_users_early = await get_users(tab="all")
            in_hero = any(u["user_key"] == test_user for u in hero_users_early)
            print(f"User {test_user} in Hero? {in_hero}")
            
            # Detail should return empty alerts
            detail_early = await get_user_alerts(test_user)
            print(f"User {test_user} alerts count: {len(detail_early['alerts'])}")
            
            # BUT RiskMeta might show future stats
            rm = detail_early["risk_meta"]
            print(f"RiskMeta max_ensemble: {rm.get('max_ensemble')}")
            print(f"RiskMeta analyst_notes count: {len(rm.get('analyst_notes', []))}")
            
            if rm.get("max_ensemble", 0) > 0 and len(detail_early['alerts']) == 0:
                print("!!! LEAK DETECTED: RiskMeta shows max_ensemble > 0 but no visible alerts.")

if __name__ == "__main__":
    asyncio.run(run_diagnosis())
