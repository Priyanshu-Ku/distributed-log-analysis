import sys
import os
from pyspark.sql.functions import col, hour, when

from src.utils.logger import logging
from src.utils.exception import CustomException
from matplotlib import pyplot as plt

class LogAnalysis:
    def __init__(self):
        pass

    def log_level_distribution(self, df):
        try:
            logging.info("Computing log level distribution")

            return df.groupBy("log_level") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def top_components(self, df):
        try:
            logging.info("Computing top components by log volume")

            return df.groupBy("component") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def warn_components(self, df):
        try:
            logging.info("Computing components generating WARN logs")

            warn_df = df.filter(col("log_level") == "WARN")

            return warn_df.groupBy("component") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def hourly_analysis(self, df):
        try:
            logging.info("Performing hourly log analysis")

            return df.withColumn("hour", hour("datetime")) \
                .groupBy("hour", "log_level") \
                .count() \
                .orderBy("hour")

        except Exception as e:
            raise CustomException(e, sys)

    def warn_spike_analysis(self, df):
        try:
            logging.info("Detecting warning spikes")

            warn_df = df.filter(col("log_level") == "WARN")

            return warn_df.withColumn("hour", hour("datetime")) \
                .groupBy("hour") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def top_failure_messages(self, df):
        try:
            logging.info("Extracting top failure messages")

            warn_df = df.filter(col("log_level") == "WARN")

            return warn_df.groupBy("message") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def process_failure_analysis(self, df):
        try:
            logging.info("Analyzing process-level failures")

            warn_df = df.filter(col("log_level") == "WARN")

            return warn_df.groupBy("process_id") \
                .count() \
                .orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def failure_risk_analysis(self, df):
        try:
            logging.info("Computing failure risk based on WARN thresholds")

            warn_df = df.filter(col("log_level") == "WARN")

            warn_hourly = warn_df.withColumn("hour", hour("datetime")) \
                .groupBy("hour") \
                .count()

            return warn_hourly.withColumn(
                "failure_risk",
                when(col("count") > 5000, "HIGH")
                .when(col("count") > 1000, "MEDIUM")
                .otherwise("LOW")
            ).orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)

    def component_risk_analysis(self, df):
        try:
            logging.info("Classifying component risk levels")

            warn_df = df.filter(col("log_level") == "WARN")

            comp_risk = warn_df.groupBy("component") \
                .count()

            return comp_risk.withColumn(
                "risk_level",
                when(col("count") > 100000, "HIGH")
                .when(col("count") > 10000, "MEDIUM")
                .otherwise("LOW")
            ).orderBy("count", ascending=False)

        except Exception as e:
            raise CustomException(e, sys)
        
        # 🔥 ================= VISUALIZATION METHODS =================

    def plot_top_components(self, df):
        try:
            comp_df = self.top_components(df).limit(10).toPandas()

            plt.figure()
            plt.bar(comp_df["component"], comp_df["count"])
            plt.xticks(rotation=45)
            plt.title("Top Components by Log Volume")
            plt.xlabel("Component")
            plt.ylabel("Count")
            plt.tight_layout()
            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "top_components.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)


    def plot_hourly_distribution(self, df):
        try:
            hour_df = df.withColumn("hour", hour("datetime")) \
                .groupBy("hour") \
                .count() \
                .orderBy("hour") \
                .toPandas()

            plt.figure()
            plt.plot(hour_df["hour"], hour_df["count"])
            plt.title("Logs per Hour")
            plt.xlabel("Hour")
            plt.ylabel("Count")
            plt.grid()
            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "hourly_distribution.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)


    def plot_warning_components(self, df):
        try:
            warn_df = self.warn_components(df).limit(10).toPandas()

            plt.figure()
            plt.bar(warn_df["component"], warn_df["count"])
            plt.xticks(rotation=45)
            plt.title("Top Warning Components")
            plt.xlabel("Component")
            plt.ylabel("WARN Count")
            plt.tight_layout()
            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "warning_components.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)


    def plot_failure_trend(self, df):
        try:
            trend_df = df.withColumn("hour", hour("datetime")) \
                .groupBy("hour") \
                .count() \
                .orderBy("hour") \
                .toPandas()

            plt.figure()
            plt.plot(trend_df["hour"], trend_df["count"])
            plt.title("Failure Trend Over Time")
            plt.xlabel("Hour")
            plt.ylabel("Log Count")
            plt.grid()
            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "failure_trend.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)


    def plot_anomalies(self, final_df):
        try:
            pdf = final_df.limit(1000).toPandas()

            plt.figure()
            plt.scatter(pdf["hour"], pdf["event_count"])
            plt.title("Anomaly Detection Scatter")
            plt.xlabel("Hour")
            plt.ylabel("Event Count")
            os.makedirs("outputs", exist_ok=True)
            plt.savefig(os.path.join("outputs", "anomaly_scatter.png"))
            plt.close()

        except Exception as e:
            raise CustomException(e, sys)
