import sys
from pyspark.sql.functions import col, concat_ws, to_timestamp, trim

from src.utils.logger import logging
from src.utils.exception import CustomException


class DataTransformation:
    def __init__(self):
        pass

    def transform(self, df):
        """
        Clean parsed log data and prepare it for analysis.
        """
        try:
            logging.info("Starting data transformation")

            # Remove empty / invalid rows
            clean_df = df.filter(
                (trim(col("date")) != "") &
                (trim(col("time")) != "") &
                (trim(col("process_id")) != "") &
                (trim(col("log_level")) != "") &
                (trim(col("component")) != "") &
                (trim(col("message")) != "")
            )

            # Cast process_id to integer
            clean_df = clean_df.withColumn("process_id", col("process_id").cast("int"))

            # Create datetime column
            clean_df = clean_df.withColumn(
                "datetime",
                to_timestamp(concat_ws(" ", col("date"), col("time")), "yyMMdd HHmmss")
            )

            # Drop rows where datetime conversion failed
            clean_df = clean_df.filter(col("datetime").isNotNull())

            logging.info("Data transformation completed successfully")
            return clean_df

        except Exception as e:
            logging.error("Error occurred during data transformation")
            raise CustomException(e, sys)