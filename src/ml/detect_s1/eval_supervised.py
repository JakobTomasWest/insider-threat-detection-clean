from pathlib import Path
import pickle

import numpy as np

from sklearn.metrics import roc_auc_score, average_precision_score, classification_report

REL = "r5.2"
ML_DIR = Path(f"out/{REL}/ml")

X_TEST_PATH = ML_DIR / "X_test.npy"
Y_TEST_PATH = ML_DIR / "y_test.npy"
MODEL_PATH = ML_DIR / "supervised_model_xgb.pkl"


def main():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Trained model not found at {MODEL_PATH}. Run train_supervised.py first.")

    print("Loading test arrays...")
    X_test = np.load(X_TEST_PATH)
    y_test = np.load(Y_TEST_PATH)

    print("Loading trained model...")
    with MODEL_PATH.open("rb") as f:
        model = pickle.load(f)

    print("Scoring test set...")
    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(X_test)[:, 1]
    else:
        # Fallback: some models may only support decision_function
        if hasattr(model, "decision_function"):
            scores = model.decision_function(X_test)
            # Min-max normalize to [0, 1] for AUC/AP compatibility
            scores_min = scores.min()
            scores_max = scores.max()
            probs = (scores - scores_min) / (scores_max - scores_min + 1e-9)
        else:
            raise AttributeError("Model does not support predict_proba or decision_function.")

    auc = roc_auc_score(y_test, probs)
    ap = average_precision_score(y_test, probs)

    print(f"Test AUC: {auc:.4f}")
    print(f"Test AP:  {ap:.4f}")

    # If we want a hard-label summary at a default threshold (0.5)
    preds = (probs >= 0.5).astype(int)
    print("\nClassification report at threshold=0.5:")
    print(classification_report(y_test, preds, digits=4))


if __name__ == "__main__":
    main()
