from pathlib import Path
import pickle

import numpy as np

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

from sklearn.metrics import roc_auc_score, average_precision_score


REL = "r5.2"
ML_DIR = Path(f"out/{REL}/ml")

X_TRAIN_PATH = ML_DIR / "X_train.npy"
X_VAL_PATH = ML_DIR / "X_val.npy"
X_TEST_PATH = ML_DIR / "X_test.npy"
Y_TRAIN_PATH = ML_DIR / "y_train.npy"
Y_VAL_PATH = ML_DIR / "y_val.npy"
Y_TEST_PATH = ML_DIR / "y_test.npy"

MODEL_PATH = ML_DIR / "supervised_model_xgb.pkl"


def load_arrays():
    """Load preprocessed feature/label arrays from disk."""
    X_train = np.load(X_TRAIN_PATH)
    X_val = np.load(X_VAL_PATH)
    X_test = np.load(X_TEST_PATH)
    y_train = np.load(Y_TRAIN_PATH)
    y_val = np.load(Y_VAL_PATH)
    y_test = np.load(Y_TEST_PATH)

    return X_train, X_val, X_test, y_train, y_val, y_test


def main():
    if XGBClassifier is None:
        raise ImportError(
            "xgboost is required for training the supervised model. "
            "Install with 'pip install xgboost'."
        )

    print("Loading preprocessed arrays...")
    X_train, X_val, X_test, y_train, y_val, y_test = load_arrays()

    print("Shapes:")
    print("  X_train:", X_train.shape, "y_train:", y_train.shape)
    print("  X_val:", X_val.shape, "y_val:", y_val.shape)
    print("  X_test:", X_test.shape, "y_test:", y_test.shape)

    # Basic XGBoost config suitable for imbalanced binary classification.
    # These can be tuned later but are a solid starting point.
    model = XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        n_jobs=4,
        scale_pos_weight=None,  # can be set to (neg/pos) later if needed
    )

    print("Training XGBoost classifier with early stopping on validation set...")

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=True,
    )

    print("Evaluating on validation and test sets...")

    # Predicted probabilities for the positive class
    val_probs = model.predict_proba(X_val)[:, 1]
    test_probs = model.predict_proba(X_test)[:, 1]

    val_auc = roc_auc_score(y_val, val_probs)
    test_auc = roc_auc_score(y_test, test_probs)
    val_ap = average_precision_score(y_val, val_probs)
    test_ap = average_precision_score(y_test, test_probs)

    print(f"Validation AUC: {val_auc:.4f}")
    print(f"Test AUC:       {test_auc:.4f}")
    print(f"Validation AP:  {val_ap:.4f}")
    print(f"Test AP:        {test_ap:.4f}")

    print(f"Saving trained model to {MODEL_PATH}...")
    with MODEL_PATH.open("wb") as f:
        pickle.dump(model, f)

    print("Training complete.")


if __name__ == "__main__":
    main()
