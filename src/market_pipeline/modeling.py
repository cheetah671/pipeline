from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MODEL_PATH = Path(__file__).resolve().parents[2] / "models" / "price_direction_model.joblib"
MODEL_FEATURE_COLUMNS = [
    "vwap",
    "realized_volatility",
    "order_book_imbalance",
    "mid_price",
]


def train_model(frame: pd.DataFrame) -> tuple[Pipeline, dict[str, float | str]]:
    usable = frame.dropna(subset=MODEL_FEATURE_COLUMNS + ["target"]).copy()
    if usable.empty:
        raise ValueError("No rows available for training")
    split_index = max(1, int(len(usable) * 0.8))
    train_frame = usable.iloc[:split_index]
    test_frame = usable.iloc[split_index:]
    feature_frame = train_frame[MODEL_FEATURE_COLUMNS]
    target = train_frame["target"]
    pipeline = Pipeline(
        steps=[
            ("scale", ColumnTransformer([("num", StandardScaler(), MODEL_FEATURE_COLUMNS)], remainder="drop")),
            ("model", LogisticRegression(max_iter=1000)),
        ]
    )
    pipeline.fit(feature_frame, target)
    metrics: dict[str, float | str] = {"train_rows": float(len(train_frame))}
    if not test_frame.empty:
        predictions = pipeline.predict(test_frame[MODEL_FEATURE_COLUMNS])
        report = classification_report(test_frame["target"], predictions, output_dict=True, zero_division=0)
        metrics.update(
            {
                "accuracy": float(report["accuracy"]),
                "macro_f1": float(report["macro avg"]["f1-score"]),
            }
        )
    return pipeline, metrics


def save_model(model: Pipeline, path: Path = MODEL_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    return path


def load_model(path: Path = MODEL_PATH) -> Pipeline:
    return joblib.load(path)


def predict(model: Pipeline, payload: dict[str, float]) -> dict[str, float]:
    features = pd.DataFrame([payload], columns=MODEL_FEATURE_COLUMNS)
    probability = float(model.predict_proba(features)[0, 1])
    return {"probability_up": probability, "prediction": float(probability >= 0.5)}
