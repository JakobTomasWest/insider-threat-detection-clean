# UI Changes - AG Pass 1 & 2

## Files Edited
*   `src/ui/app.py`
*   `src/ui/static/app.js`
*   `src/ui/static/index.html`

## Summary of Changes
### Backend
*   **LDAP Enrichment**: Implemented `load_ldap_metadata` to load user names and roles from `out/r5.2/ldap_v3_full/ldap_asof_by_month.parquet`.
*   **API Updates**:
    *   `/api/users`: Returns `name`, `role`, and `is_terminated` status.
    *   `/api/alerts/{id}`: Returns `user_name`, `user_role`, `user_last_seen`, and `is_terminated`.
    *   **Bug Fix**: Added `_check_terminated` helper to safely compare `STATE.current_day` (string) and `last_seen` (datetime/date).

### Frontend
*   **Human-Readable Text**:
    *   Replaced `SCENARIO_1` with "Data Theft" (escalated) or "Potential Future Data Theft" (non-escalated).
    *   Replaced raw user keys with "Full Name (user_key)" in sidebar and modal.
    *   Formatted all dates as MM-DD-YYYY using `formatDate` helper.
*   **Alert Modal**:
    *   Renamed "How the Score Was Computed" to "Alert Description".
    *   Added "Detector Findings" section with specific mappings:
        *   `s1_chain_post_departure` -> "Confirmed data theft after termination..."
        *   `s1_chain` -> "Strong evidence of data theft while employed..."
        *   `s1_near_miss` -> "High-risk activity consistent with data theft..."
        *   `none` -> "No rule-based evidence..."
    *   Added **Forecast** detector card.
    *   Updated Anomaly card to say "Anomaly detector is not yet active for this scenario" when not wired.
*   **Heartbeat Visualization**:
    *   Dynamic Title:
        *   System: "Alert Volume (7-Day Window)"
        *   Watchlist (Aggregate): "Forecast Risk (Watchlist, 7-Day Window)"
        *   Watchlist (User): "Forecast score over time for <Full Name>"
    *   Dynamic Legend: Shows relevant series based on mode.
*   **Cache Busting**: Added `?v=4` to `app.js` and `style.css` in `index.html`.
*   **Bug Fix**: Restored missing `openModal` function declaration in `app.js`.

## Backend TODOs
*   None.
