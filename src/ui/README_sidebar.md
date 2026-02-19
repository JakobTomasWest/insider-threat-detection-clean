# Sidebar & Modal Identity Display

This document explains the display logic for user identity in the sidebar and modal.

## Sidebar

### Alerts Tab
*   **Line 1:** Severity Dot | Full Name
*   **Line 2:** `(user_key)` (muted) | Badges (Total, Escalated)
*   **Role:** Not displayed.

### Watchlist Tab
*   **Line 1:** Full Name
*   **Line 2:** Badges (F:Score, Total Alerts)
*   **Line 3:** `(user_key)` (muted)
*   **Role:** Not displayed.

## Alert Details Modal

### Header
*   **Top Line:** Full Name · Role (e.g., "Hedy Rhoda Estrada · IT Admin")
*   **Meta Line:** Date | Scenario
*   **User ID:** Not displayed anywhere in the modal.

### Role Formatting
*   Roles are formatted to be human-readable (e.g., "ITAdmin" -> "IT Admin").

## Global Rules
*   **User ID (`user_key`)**: Displayed in sidebar (muted). Hidden in modal.
*   **Role**: Displayed in Modal Header. Hidden in Sidebar.
