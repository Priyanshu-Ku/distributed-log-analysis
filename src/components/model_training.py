import sys
import os
import numpy as np
import matplotlib.pyplot as plt

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.components.feature_builder import build_window_features
from src.utils.logger import logging
from src.utils.exception import CustomException


class LogModel:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()

        self.feature_cols = [
            "event_count",
            "warn_count",
            "unique_components",
            "unique_processes",
            "warn_ratio",
            "event_delta",
            "rolling_mean",
            "warn_intensity"
        ]

    # ============================================================
    # 🔥 STEP 1: Feature Preparation (Window-Based)
    # ============================================================
    def prepare_features(self, df):
        try:
            logging.info("Preparing window-based features")

            features_df = build_window_features(df)

            pdf = features_df.toPandas()

            # Sort for time-series consistency
            pdf = pdf.sort_values("bucket_start").reset_index(drop=True)

            # Handle edge cases
            pdf = pdf.fillna(0)

            logging.info(f"Prepared {len(pdf)} feature rows")
            return pdf

        except Exception as e:
            raise CustomException(e, sys)

    # ============================================================
    # 🔥 STEP 2: Time-based Train-Test Split
    # ============================================================
    def train_test_split(self, pdf, split_ratio=0.7):
        try:
            split_idx = int(len(pdf) * split_ratio)

            train_df = pdf.iloc[:split_idx].copy()
            test_df = pdf.iloc[split_idx:].copy()

            logging.info(f"Train size: {len(train_df)}, Test size: {len(test_df)}")

            return train_df, test_df

        except Exception as e:
            raise CustomException(e, sys)

    # ============================================================
    # 🔥 STEP 3: Train Model
    # ============================================================
    def train_model(self, train_df):
        try:
            logging.info("Training Isolation Forest")

            X_train = train_df[self.feature_cols].astype("float32")

            # 🔥 Scale features
            X_scaled = self.scaler.fit_transform(X_train)

            self.model = IsolationForest(
                n_estimators=150,
                contamination=0.1,
                random_state=42
            )

            self.model.fit(X_scaled)

            logging.info("Model trained successfully")

        except Exception as e:
            raise CustomException(e, sys)

    # ============================================================
    # 🔥 STEP 4: Predict
    # ============================================================
    def predict(self, test_df):
        try:
            logging.info("Predicting anomalies on test data")

            X_test = test_df[self.feature_cols].astype("float32")

            # Scale features
            X_scaled = self.scaler.transform(X_test)

            scores = self.model.decision_function(X_scaled)
            threshold = np.percentile(scores, 10)

            test_df["anomaly_score"] = scores
            test_df["anomaly"] = np.where(scores < threshold, "ANOMALY", "NORMAL")

            test_df["rule_flag"] = np.where(
                test_df["warn_ratio"] > 0.2, 1, 0
            )

            test_df["final_anomaly"] = np.where(
                (test_df["anomaly"] == "ANOMALY") | (test_df["rule_flag"] == 1),
                "ANOMALY",
                "NORMAL"
            )

            return test_df

        except Exception as e:
            raise CustomException(e, sys)

    # ============================================================
    # 🔥 STEP 5: Evaluate
    # ============================================================
    def evaluate(self, pdf):
        try:
            logging.info("Evaluating model")

            total = len(pdf)
            anomalies = len(pdf[pdf["anomaly"] == "ANOMALY"])

            percentage = (anomalies / total) * 100 if total > 0 else 0

            print("\n=== Model Evaluation ===")
            print(f"Total: {total}")
            print(f"Anomalies: {anomalies}")
            print(f"Percentage: {percentage:.2f}%")

            return {
                "total": total,
                "anomalies": anomalies,
                "percentage": percentage
            }

        except Exception as e:
            raise CustomException(e, sys)

    # ============================================================
    # 🔥 STEP 6: Visualization
    # ============================================================
    def visualize(self, pdf):
        try:
            logging.info("Visualizing anomalies with scores")

            pdf = pdf.reset_index(drop=True)
            pdf["time_index"] = range(len(pdf))

            normal = pdf[pdf["anomaly"] == "NORMAL"]
            anomaly = pdf[pdf["anomaly"] == "ANOMALY"]

            # =====================================================
            # 🔥 Plot 1: Event Count vs Time
            # =====================================================
            plt.figure()

            plt.scatter(normal["time_index"], normal["event_count"], label="Normal")
            plt.scatter(anomaly["time_index"], anomaly["event_count"], label="Anomaly")

            plt.xlabel("Time Window Index")
            plt.ylabel("Event Count")
            plt.title("Anomaly Detection in Logs (Window-Based)")
            plt.legend()

            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "anomaly_event_count.png"))
            plt.close()

            # =====================================================
            # 🔥 Plot 2: Anomaly Score Trend (NEW)
            # =====================================================
            plt.figure()

            plt.plot(pdf["time_index"], pdf["anomaly_score"], marker="o")

            # Highlight anomalies
            plt.scatter(anomaly["time_index"], anomaly["anomaly_score"], label="Anomaly")

            plt.xlabel("Time Window Index")
            plt.ylabel("Anomaly Score")
            plt.title("Anomaly Score Trend (Lower = More Anomalous)")
            plt.legend()

            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "anomaly_score_trend.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)
        
        
    def explain_anomalies(self, pdf):
        try:
            logging.info("Explaining anomalies")

            # Split
            normal_df = pdf[pdf["anomaly"] == "NORMAL"]
            anomaly_df = pdf[pdf["anomaly"] == "ANOMALY"]

            explanations = []

            for idx, row in anomaly_df.iterrows():
                explanation = {"index": idx}

                for col in self.feature_cols:
                    normal_mean = normal_df[col].mean()

                    if normal_mean == 0:
                        deviation = 0
                    else:
                        deviation = (row[col] - normal_mean) / normal_mean

                    explanation[col] = deviation

                explanations.append(explanation)

            import pandas as pd
            explanation_df = pd.DataFrame(explanations)

            print("\n=== Anomaly Explanations (Top Deviations) ===")
            print(explanation_df.head())

            return explanation_df

        except Exception as e:
            raise CustomException(e, sys)
        
        
    def visualize_feature_contribution(self, pdf):
        try:
            logging.info("Visualizing feature contribution")

            anomaly_df = pdf[pdf["anomaly"] == "ANOMALY"]

            if anomaly_df.empty:
                print("No anomalies to explain")
                return

            # Take first anomaly for demo
            sample = anomaly_df.iloc[0]

            values = [sample[col] for col in self.feature_cols]

            plt.figure()
            plt.bar(self.feature_cols, values)

            plt.title("Feature Contribution for Anomaly (Sample)")
            plt.ylabel("Feature Value")
            plt.xticks(rotation=45)

            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "feature_contribution.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)
