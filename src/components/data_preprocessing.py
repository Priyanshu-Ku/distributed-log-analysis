from pyspark.sql.functions import regexp_extract
from src.utils.logger import logging
from src.utils.exception import CustomException
import sys


class LogPreprocessor:
    def __init__(self):
        pass

    def parse_logs(self, df):
        try:
            logging.info("Starting log parsing")

            parsed_df = df.select(
                regexp_extract('value', r'^(\d+)', 1).alias("date"),
                regexp_extract('value', r'^\d+\s+(\d+)', 1).alias("time"),
                regexp_extract('value', r'^\d+\s+\d+\s+(\d+)', 1).alias("process_id"),
                regexp_extract('value', r'\b(INFO|ERROR|WARN|DEBUG)\b', 1).alias("log_level"),
                regexp_extract('value', r'([a-zA-Z0-9.$]+):', 1).alias("component"),
                regexp_extract('value', r':\s+(.*)', 1).alias("message")
            )

            logging.info("Log parsing completed")

            return parsed_df

        except Exception as e:
            raise CustomException(e, sys)