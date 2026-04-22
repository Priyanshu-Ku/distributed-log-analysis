import sys
import matplotlib.pyplot as plt

from pyspark.sql.functions import hour

from sklearn.ensemble import IsolationForest

from src.utils.logger import logging
from src.utils.exception import CustomException


class LogModel:
    def __init__(self):
        self.model = None

    def prepare_features(self, df):
        try:
            logging.info("Preparing features")

            feature_df = df.withColumn("hour", hour("datetime"))

            feature_df = feature_df.groupBy("hour") \
                .agg({"process_id": "count"}) \
                .withColumnRenamed("count(process_id)", "event_count")

            pdf = feature_df.toPandas()

            logging.info("Feature preparation done")
            return pdf

        except Exception as e:
            raise CustomException(e, sys)

    def train_model(self, pdf):
        try:
            logging.info("Training Isolation Forest")

            X = pdf[["hour", "event_count"]]

            self.model = IsolationForest(
                n_estimators=100,
                contamination=0.1,
                random_state=42
            )

            self.model.fit(X)

            logging.info("Model trained successfully")
            return self.model

        except Exception as e:
            raise CustomException(e, sys)

    def predict(self, pdf):
        try:
            logging.info("Predicting anomalies")

            X = pdf[["hour", "event_count"]]

            pdf["anomaly"] = self.model.predict(X)
            pdf["anomaly"] = pdf["anomaly"].apply(
                lambda x: "ANOMALY" if x == -1 else "NORMAL"
            )

            return pdf

        except Exception as e:
            raise CustomException(e, sys)

    def evaluate(self, pdf):
        try:
            logging.info("Evaluating model")

            total = len(pdf)
            anomalies = len(pdf[pdf["anomaly"] == "ANOMALY"])

            percentage = (anomalies / total) * 100

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

    def visualize(self, pdf):
        try:
            logging.info("Visualizing anomalies")

            normal = pdf[pdf["anomaly"] == "NORMAL"]
            anomaly = pdf[pdf["anomaly"] == "ANOMALY"]

            plt.figure()

            plt.scatter(normal["hour"], normal["event_count"], label="Normal")
            plt.scatter(anomaly["hour"], anomaly["event_count"], label="Anomaly")

            plt.xlabel("Hour")
            plt.ylabel("Event Count")
            plt.title("Anomaly Detection in Logs")
            plt.legend()

            plt.show()

        except Exception as e:
            raise CustomException(e, sys)