# Data Processing and Detection Pipeline

This document describes the end-to-end pipeline used to transform raw CERT enterprise activity logs into insider threat alerts and dashboard-ready investigation artifacts.

The current pipeline is driven by project scripts and application code rather than the older notebook-only workflow. At a high level, the system:

1. ingests raw CERT logs
2. cleans and normalizes source events
3. builds derived behavioral features
4. applies rule-based, anomaly, and supervised ML detectors
5. combines detector outputs into ensemble risk scores
6. writes alert artifacts for downstream investigation in the UI

---

## Pipeline Data Flow


```text
Raw CERT Logs (CSV)
   │
   ▼
Event Parsing & Normalization
(clean timestamps, user IDs, schemas)
   │
   ▼
Unified Event Tables (DuckDB / Parquet)
   │
   ▼
User-Day Aggregation
(events grouped per user per day)
   │
   ▼
Feature Engineering
(features_v1 → features_v2, 14-day sliding windows)
   │
   ▼
Detector Inputs
(per-user, per-day feature vectors)
   │
   ▼
Detection Outputs
- rule triggers (boolean flags)
- anomaly scores (deviation metrics)
- ML probabilities
   │
   ▼
Ensemble Risk Scoring
(weighted combination of signals)
   │
   ▼
Alert Records (NDJSON)
(user, day, score, contributing signals)
   │
   ▼
Dashboard UI (FastAPI)
(interactive investigation + visualization)
```

---

## Pipeline Inputs

The pipeline begins with raw activity logs from the **CERT Insider Threat Dataset (r5.2)**.

Primary source data includes:

- logon activity
- HTTP browsing activity
- email communication
- file transfer activity
- USB / device activity
- LDAP employee records

These sources provide both user behavior and organizational context.

---

## Pipeline Stages

### 1. Raw Data Ingestion

The system reads the raw CERT log files from `data/r5.2/` and prepares them for downstream processing.

The ingestion layer is responsible for:

- locating the configured dataset release
- loading source logs into a consistent processing workflow
- preparing source data for ETL and feature generation

At this stage, the data is still source-specific and not yet standardized across event types.

---

### 2. Data Cleaning and Normalization

Raw source logs are cleaned and normalized so that multiple event types can be analyzed together.

Typical preprocessing operations include:

- timestamp normalization
- username / identifier standardization
- event formatting cleanup
- removal or handling of malformed records
- alignment of event records with organizational context from LDAP

This stage produces cleaner intermediate datasets that can be queried consistently.

---

### 3. Derived Event Tables and Analytical Artifacts

After normalization, the pipeline builds derived event tables and analytical artifacts used throughout the rest of the system.

In practice, this stage supports:

- efficient downstream queries
- reproducible feature generation
- consistent joins across event sources
- scalable analysis on large log files

These artifacts are typically stored as derived outputs under `out/` and may include Parquet-backed tables or other intermediate data products used by later pipeline stages.

---

### 4. Feature Engineering

Behavioral features are derived from historical user activity and aggregated across time windows.

The resulting features are stored as `features_v2` datasets and serve as the primary input for all detection components.

Examples of feature categories include:

- login frequency and timing
- abnormal login hours
- file access and transfer behavior
- browsing activity patterns
- deviations from historical user baselines
- sliding-window behavioral summaries

Many of these features are computed over rolling or windowed time periods so the system can capture behavior trends rather than only isolated events.

---

### 5. Detection Layer

The detection layer applies multiple strategies to the engineered behavioral features.

#### Rule-Based Detection

The rule engine detects explicit suspicious behaviors and known insider patterns.

Examples include:

- risky file transfer behavior
- suspicious USB activity
- policy-violation style event chains
- known scenario-oriented behavioral patterns

This detector provides strong, interpretable signals for behaviors that are explicitly encoded in the rules.

#### Anomaly Detection

The anomaly detector identifies deviations from normal historical behavior.

Its purpose is to surface unusual user activity even when that activity does not exactly match a predefined rule pattern.

Typical anomaly-style signals include:

- unusual behavioral spikes
- deviations from user baselines
- outlier behavior relative to historical trends

#### Machine Learning Detection

The supervised ML layer uses engineered behavioral features to learn complex patterns associated with insider threat scenarios.

The project uses supervised classification models to capture signals that are difficult to express as deterministic rules alone.

This detector is especially useful for:

- multi-feature behavioral interactions
- probabilistic risk estimation
- earlier detection of risky patterns before explicit exfiltration behavior is complete

---

### 6. Ensemble Risk Scoring

Outputs from the rule engine, anomaly detector, and machine learning models are combined into a unified risk score.

The ensemble stage exists because each detector captures a different type of signal:

- **rules** capture explicit known patterns
- **anomaly detection** captures deviations from expected behavior
- **machine learning** captures higher-order behavioral patterns

By combining them, the system improves robustness and can prioritize users who exhibit multiple forms of suspicious behavior.

The resulting ensemble score is used to determine which users and events should surface as alerts.

---

### 7. Alert Generation

Once risk scores are computed, the system generates structured alert artifacts for downstream investigation.

These alert outputs may contain:

- user identifier
- alert timestamp or day
- detector contributions
- alert reason or scenario context
- ensemble or detector-level scores

In the current project workflow, alert outputs are written in formats suitable for downstream UI loading and investigation workflows, including NDJSON-style artifacts used by the dashboard.

---

### 8. Visualization and Investigation

Generated alerts are loaded into the monitoring dashboard, which provides an analyst-facing interface for triage and investigation.

The UI supports activities such as:

- viewing high-risk users
- inspecting triggered alerts
- reviewing activity timelines
- understanding contributing behavioral signals
- exploring detector outputs and investigation context

This stage turns model output into something actionable for a human investigator.

---

## How the Pipeline Is Run

At a practical level, the repo’s current workflow is script-driven.

Typical execution work flow:

1. create and activate a virtual environment
2. install dependencies from `requirements.txt`
3. place the CERT dataset under `data/r5.2/`
4. run the build pipeline using project scripts such as `scripts/build_all.sh`
5. generate alerts under `out/`
6. start the dashboard with `make ui`

For environment and setup details, see [`SETUP.md`](SETUP.md).

---

## Key Output Artifacts

The pipeline ultimately produces artifacts that support investigation and evaluation, including:

- derived datasets under `out/`
- alert artifacts used by the dashboard
- intermediate processed datasets used by the detectors


The most important practical output for the UI is the generated alert data that the FastAPI dashboard reads at startup.

The outputs of this pipeline are used to compute detection metrics and evaluation results described in `results.md`.


---

## Design Goals

The pipeline is designed to support:

- reproducible data processing
- modular detection components
- scalable analysis over large enterprise log datasets
- explainable investigation outputs
- extension with additional detectors or feature sets

This modular structure allows the system to evolve without redesigning the entire pipeline.