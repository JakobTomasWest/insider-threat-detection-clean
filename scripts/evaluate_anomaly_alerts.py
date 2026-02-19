#!/usr/bin/env python3
"""
Anomaly Detection Alert Evaluator

Evaluates anomaly detection alerts using metrics optimized for unsupervised learning:
- ROC-AUC and PR-AUC (Area Under Curve metrics)
- Precision, Recall, F1 at different severity levels
- Alert workload analysis (FP per TP ratio)
- Temporal coverage analysis
- User-level detection rate
- Precision at different recall levels

Run: python3 src/detector/evaluate_alerts.py
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve, roc_curve

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALERTS_PATH = PROJECT_ROOT / "out" / "r5.2" / "anomaly" / "evaluation" / "test_alerts.json"
TEST_SCORES_PATH = PROJECT_ROOT / "out" / "r5.2" / "anomaly" / "evaluation" / "test_scores.parquet"
ANSWERS_DIR = PROJECT_ROOT / "answers" / "r5.2-1"


def load_ground_truth():
    """Load Scenario 1 ground truth (malicious user-days) filtered to test set only."""
    print("\nLoading ground truth...")
    
    # Load test set to see which users we actually tested on
    if TEST_SCORES_PATH.exists():
        test_scores = pd.read_parquet(TEST_SCORES_PATH)
        test_users = set(test_scores['user_key'].unique())
        print(f"  Test set contains {len(test_users)} total users")
    else:
        print(f"  WARNING: Cannot load test scores from {TEST_SCORES_PATH}")
        test_users = None
    
    malicious_events = []
    s1_users_found = set()
    s1_users_in_test = set()
    
    for csv_file in ANSWERS_DIR.glob("*.csv"):
        user = csv_file.stem.split('-')[-1].lower()
        s1_users_found.add(user)
        
        # Only include S1 users that are in the test set
        if test_users is not None and user not in test_users:
            continue
        
        s1_users_in_test.add(user)
        
        # Parse raw event logs (no header, comma-delimited)
        with open(csv_file) as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 3:
                    # Format: event_type,id,timestamp,user,...
                    timestamp_str = parts[2]
                    try:
                        day = pd.to_datetime(timestamp_str).strftime('%Y-%m-%d')
                        malicious_events.append({
                            'user': user,
                            'day': day,
                            'user_day': f"{user}_{day}"
                        })
                    except:
                        continue
    
    # Deduplicate to get unique user-days
    gt_df = pd.DataFrame(malicious_events).drop_duplicates('user_day')
    
    print(f"  Total S1 malicious users: {len(s1_users_found)}")
    print(f"  S1 users in TEST set: {len(s1_users_in_test)} ({sorted(s1_users_in_test)})")
    print(f"  S1 users in TRAIN/VAL: {len(s1_users_found - s1_users_in_test)}")
    print(f"  Ground truth (test set only): {len(gt_df)} malicious user-days from {gt_df['user'].nunique()} users")
    
    return gt_df


def load_alerts():
    """Load anomaly detection alerts."""
    print("\nLoading alerts...")
    
    if not ALERTS_PATH.exists():
        print(f"  ERROR: Alerts file not found at {ALERTS_PATH}")
        return None
    
    with open(ALERTS_PATH) as f:
        alerts = json.load(f)
    
    alerts_df = pd.DataFrame(alerts)
    alerts_df['user_day'] = alerts_df['user_key'] + '_' + alerts_df['day']
    
    print(f"  Loaded {len(alerts_df)} alerts")
    print(f"  Unique users: {alerts_df['user_key'].nunique()}")
    print(f"  Date range: {alerts_df['day'].min()} to {alerts_df['day'].max()}")
    print(f"  Severity breakdown:")
    for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        count = (alerts_df['severity'] == severity).sum()
        pct = count / len(alerts_df) * 100
        print(f"    {severity}: {count} ({pct:.1f}%)")
    
    return alerts_df


def compute_metrics(gt_df, alerts_df, severity_filter=None):
    """Compute precision, recall, F1 for given severity threshold."""
    if severity_filter:
        alerts_filtered = alerts_df[alerts_df['severity'].isin(severity_filter)]
        label = '+'.join(severity_filter)
    else:
        alerts_filtered = alerts_df
        label = "ALL"
    
    predicted_user_days = set(alerts_filtered['user_day'])
    actual_user_days = set(gt_df['user_day'])
    
    tp = len(predicted_user_days & actual_user_days)
    fp = len(predicted_user_days - actual_user_days)
    fn = len(actual_user_days - predicted_user_days)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    
    fp_per_tp = fp / tp if tp > 0 else float('inf')
    
    return {
        'label': label,
        'alerts': len(alerts_filtered),
        'tp': tp,
        'fp': fp,
        'fn': fn,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'fp_per_tp': fp_per_tp
    }


def user_level_analysis(gt_df, alerts_df):
    """Analyze detection at user level (did we catch each malicious user?)."""
    print("\n" + "="*80)
    print(" USER-LEVEL DETECTION ANALYSIS")
    print("="*80)
    
    gt_users = set(gt_df['user'].unique())
    alert_users = set(alerts_df['user_key'].unique())
    
    detected_users = gt_users & alert_users
    missed_users = gt_users - alert_users
    
    user_recall = len(detected_users) / len(gt_users) if len(gt_users) > 0 else 0
    
    print(f"\n  Malicious users in test set: {len(gt_users)}")
    print(f"  Detected: {len(detected_users)} ({user_recall:.1%})")
    print(f"  Missed: {len(missed_users)}")
    
    if missed_users:
        print(f"\n  Missed users: {sorted(missed_users)}")
    
    # Analyze per-user detection depth
    print(f"\n  Per-user detection depth:")
    user_detection = []
    for user in detected_users:
        user_gt_days = gt_df[gt_df['user'] == user]['user_day']
        user_alerts = alerts_df[alerts_df['user_key'] == user]
        user_detected_days = set(user_alerts['user_day']) & set(user_gt_days)
        detection_rate = len(user_detected_days) / len(user_gt_days)
        user_detection.append({
            'user': user,
            'total_days': len(user_gt_days),
            'detected_days': len(user_detected_days),
            'rate': detection_rate,
            'alerts': len(user_alerts)
        })
    
    user_df = pd.DataFrame(user_detection).sort_values('rate', ascending=False)
    print(f"    Perfect detection (100%): {(user_df['rate'] == 1.0).sum()} users")
    print(f"    High detection (≥80%): {(user_df['rate'] >= 0.8).sum()} users")
    print(f"    Partial detection (≥50%): {(user_df['rate'] >= 0.5).sum()} users")
    print(f"    Low detection (<50%): {(user_df['rate'] < 0.5).sum()} users")
    
    return user_df


def temporal_analysis(gt_df, alerts_df):
    """Analyze detection over time."""
    print("\n" + "="*80)
    print(" TEMPORAL ANALYSIS")
    print("="*80)
    
    gt_df['date'] = pd.to_datetime(gt_df['day'])
    alerts_df['date'] = pd.to_datetime(alerts_df['day'])
    
    gt_by_month = gt_df.groupby(gt_df['date'].dt.to_period('M')).size()
    alerts_by_month = alerts_df.groupby(alerts_df['date'].dt.to_period('M')).size()
    
    print(f"\n  Monthly malicious events vs alerts:")
    for month in sorted(set(gt_by_month.index) | set(alerts_by_month.index)):
        gt_count = gt_by_month.get(month, 0)
        alert_count = alerts_by_month.get(month, 0)
        print(f"    {month}: {gt_count} malicious events, {alert_count} alerts")


def score_distribution_analysis(alerts_df):
    """Analyze score and boost distributions."""
    print("\n" + "="*80)
    print(" SCORE DISTRIBUTION ANALYSIS")
    print("="*80)
    
    base_scores = [a['evidence']['anomaly_score_raw'] for a in alerts_df.to_dict('records')]
    boosted_scores = [a['evidence']['anomaly_score_boosted'] for a in alerts_df.to_dict('records')]
    boost_pcts = [a['evidence'].get('boost_pct', 0) for a in alerts_df.to_dict('records')]
    
    print(f"\n  Base anomaly scores:")
    print(f"    Mean: {np.mean(base_scores):.4f}")
    print(f"    Median: {np.median(base_scores):.4f}")
    print(f"    Min: {np.min(base_scores):.4f}, Max: {np.max(base_scores):.4f}")
    
    print(f"\n  Boosted scores:")
    print(f"    Mean: {np.mean(boosted_scores):.4f}")
    print(f"    Median: {np.median(boosted_scores):.4f}")
    
    print(f"\n  Boost percentages:")
    print(f"    Mean boost: {np.mean(boost_pcts):.1f}%")
    print(f"    Alerts with boost >0: {sum(1 for b in boost_pcts if b > 0)} ({sum(1 for b in boost_pcts if b > 0)/len(boost_pcts)*100:.1f}%)")


def pattern_analysis(alerts_df):
    """Analyze which attack patterns are being detected."""
    print("\n" + "="*80)
    print(" ATTACK PATTERN ANALYSIS")
    print("="*80)
    
    wikileaks_alerts = sum(1 for a in alerts_df.to_dict('records') if a['evidence'].get('wikileaks_hits_14d', 0) > 0)
    high_usb_alerts = sum(1 for a in alerts_df.to_dict('records') 
                          if a['evidence'].get('usb_novelty_sum', 0) > 0 
                          or 'USB' in a.get('reason', ''))
    file_exfil_alerts = sum(1 for a in alerts_df.to_dict('records') if a['evidence'].get('file_to_removable_sum', 0) > 0)
    
    print(f"\n  Pattern coverage:")
    print(f"    WikiLeaks access: {wikileaks_alerts} alerts ({wikileaks_alerts/len(alerts_df)*100:.1f}%)")
    print(f"    USB activity: {high_usb_alerts} alerts ({high_usb_alerts/len(alerts_df)*100:.1f}%)")
    print(f"    File exfiltration: {file_exfil_alerts} alerts ({file_exfil_alerts/len(alerts_df)*100:.1f}%)")


def unsupervised_metrics(gt_df):
    """Calculate unsupervised learning metrics (AUC, PR-AUC, etc.)."""
    print("\n" + "="*80)
    print(" UNSUPERVISED LEARNING METRICS (AUC, PR-AUC)")
    print("="*80)
    
    if not TEST_SCORES_PATH.exists():
        print(f"\n  ⚠️  Test scores file not found at {TEST_SCORES_PATH}")
        print(f"     Cannot compute AUC/PR-AUC without full test set scores.")
        print(f"     These metrics are shown during training in anomaly_base.py")
        return
    
    # Load test set with scores
    print("\n  Loading test set scores...")
    test_df = pd.read_parquet(TEST_SCORES_PATH)
    print(f"  Test samples: {len(test_df)}")
    
    # Create ground truth labels
    test_df['user_day'] = test_df['user_key'] + '_' + test_df['day']
    gt_user_days = set(gt_df['user_day'])
    test_df['is_malicious'] = test_df['user_day'].isin(gt_user_days).astype(int)
    
    malicious_count = test_df['is_malicious'].sum()
    normal_count = len(test_df) - malicious_count
    
    print(f"  Malicious windows: {malicious_count}")
    print(f"  Normal windows: {normal_count}")
    print(f"  Imbalance ratio: {normal_count/malicious_count:.1f}:1")
    
    # Calculate AUC metrics
    y_true = test_df['is_malicious'].values
    y_scores = test_df['anomaly_score_boosted'].values
    
    try:
        roc_auc = roc_auc_score(y_true, y_scores)
        avg_precision = average_precision_score(y_true, y_scores)
        
        print(f"\n  ROC-AUC Score: {roc_auc:.4f}")
        print(f"  Average Precision (PR-AUC): {avg_precision:.4f}")
        
        # Precision at different recall levels
        precisions, recalls, thresholds = precision_recall_curve(y_true, y_scores)
        
        print(f"\n  Precision at different recall levels:")
        for target_recall in [0.10, 0.20, 0.50, 0.80, 0.90]:
            idx = np.where(recalls >= target_recall)[0]
            if len(idx) > 0:
                precision_at_recall = precisions[idx[0]]
                threshold_at_recall = thresholds[idx[0]-1] if idx[0] > 0 else thresholds[0]
                print(f"    Recall={target_recall:.0%}: Precision={precision_at_recall:.2%} (threshold={threshold_at_recall:.4f})")
        
        # Alert workload at different thresholds
        print(f"\n  Alert volume vs detection rate:")
        for percentile in [90, 95, 99, 99.5]:
            threshold = np.percentile(y_scores, percentile)
            predicted = (y_scores >= threshold).astype(int)
            alerts_count = predicted.sum()
            detected = (predicted & y_true).sum()
            precision = detected / alerts_count if alerts_count > 0 else 0
            recall = detected / malicious_count if malicious_count > 0 else 0
            
            print(f"    {percentile}th percentile (threshold={threshold:.4f}): {alerts_count} alerts, {detected} TPs, Precision={precision:.2%}, Recall={recall:.2%}")
    
    except Exception as e:
        print(f"\n  ⚠️  Error calculating metrics: {e}")
        print(f"     This may happen if there are no malicious samples in test set")


def main():
    print("="*80)
    print(" ANOMALY DETECTION EVALUATION")
    print("="*80)
    
    gt_df = load_ground_truth()
    alerts_df = load_alerts()
    
    if alerts_df is None:
        return
    
    # Overall metrics
    print("\n" + "="*80)
    print(" OVERALL METRICS")
    print("="*80)
    
    metrics_all = compute_metrics(gt_df, alerts_df)
    
    print(f"\n  ALL ALERTS:")
    print(f"    Total alerts: {metrics_all['alerts']}")
    print(f"    True Positives: {metrics_all['tp']}")
    print(f"    False Positives: {metrics_all['fp']}")
    print(f"    False Negatives: {metrics_all['fn']}")
    print(f"    Precision: {metrics_all['precision']:.2%}")
    print(f"    Recall: {metrics_all['recall']:.2%}")
    print(f"    F1 Score: {metrics_all['f1']:.2%}")
    print(f"    FP per TP: {metrics_all['fp_per_tp']:.1f} (investigate {metrics_all['fp_per_tp']+1:.0f} alerts to find 1 attack)")
    
    # Severity-based metrics
    print("\n" + "="*80)
    print(" SEVERITY-BASED METRICS")
    print("="*80)
    
    severity_configs = [
        (['CRITICAL'], "CRITICAL only"),
        (['CRITICAL', 'HIGH'], "CRITICAL + HIGH"),
        (['CRITICAL', 'HIGH', 'MEDIUM'], "CRITICAL + HIGH + MEDIUM"),
    ]
    
    print(f"\n  {'Severity':<25} {'Alerts':<8} {'Precision':<12} {'Recall':<10} {'F1':<10} {'FP/TP':<10}")
    print(f"  {'-'*25} {'-'*8} {'-'*12} {'-'*10} {'-'*10} {'-'*10}")
    
    for severity_list, label in severity_configs:
        metrics = compute_metrics(gt_df, alerts_df, severity_list)
        print(f"  {label:<25} {metrics['alerts']:<8} {metrics['precision']:>10.1%}  {metrics['recall']:>8.1%}  {metrics['f1']:>8.1%}  {metrics['fp_per_tp']:>8.1f}")
    
    # Additional analyses
    user_df = user_level_analysis(gt_df, alerts_df)
    temporal_analysis(gt_df, alerts_df)
    score_distribution_analysis(alerts_df)
    pattern_analysis(alerts_df)
    unsupervised_metrics(gt_df)
    
    # Summary for SOC workload
    print("\n" + "="*80)
    print(" SOC WORKLOAD SUMMARY")
    print("="*80)
    
    critical_high = compute_metrics(gt_df, alerts_df, ['CRITICAL', 'HIGH'])
    
    print(f"\n  Recommended triage strategy: Focus on CRITICAL + HIGH")
    print(f"    Alerts to investigate: {critical_high['alerts']}")
    print(f"    Expected TPs: {critical_high['tp']}")
    print(f"    Expected FPs: {critical_high['fp']}")
    print(f"    Workload: {critical_high['fp_per_tp']:.1f} FPs per TP")
    print(f"    Detection rate: {critical_high['recall']:.1%} of malicious user-days")
    print(f"    Precision: {critical_high['precision']:.1%}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
