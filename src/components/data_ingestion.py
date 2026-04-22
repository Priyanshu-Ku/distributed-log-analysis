import sys
from pyspark.sql import SparkSession

from src.utils.logger import logging
from src.utils.exception import CustomException


class DataIngestion:
    def __init__(self):
        self.spark = None

    def start_spark_session(self):
        try:
            logging.info("Starting Spark Session")

            self.spark = (
                SparkSession.builder
                .appName("DistributedLogAnalysis")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.shuffle.partitions", "50")
                .getOrCreate()
            )

            logging.info("Spark Session started successfully")
            return self.spark

        except Exception as e:
            logging.error("Error while starting Spark session")
            raise CustomException(e, sys)

    def load_data(self, file_path: str):
        try:
            if self.spark is None:
                self.start_spark_session()

            logging.info(f"Loading data from path: {file_path}")

            df = self.spark.read.text(file_path)

            logging.info("Data loaded successfully")
            return df

        except Exception as e:
            logging.error("Error while loading data")
            raise CustomException(e, sys)
        
# from pyspark.sql import SparkSession
# from src.utils.logger import logging
# from src.utils.exception import CustomException
# import sys
# import os

# class DataIngestion:
#     def __init__(self):
#         self.spark = None

#     def start_spark_session(self):
#         try:
#             logging.info("Starting Spark Session")

#             self.spark = SparkSession.builder \
#                 .appName("DistributedLogAnalysis") \
#                 .config("spark.driver.memory", "2g") \
#                 .config("spark.executor.memory", "2g") \
#                 .config("spark.sql.shuffle.partitions", "50") \
#                 .getOrCreate()

#             logging.info("Spark Session Started Successfully")

#         except Exception as e:
#             raise CustomException(e, sys)


#     # def load_data(self, file_path: str):
#     #     try:
#     #         base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
#     #         full_path = os.path.join(base_dir, file_path)
#     #         logging.info(f"Loading data from {full_path}")
#     #         df = self.spark.read.text(full_path)
#     #         logging.info("Data loaded successfully")
#     #         return df

#     #     except Exception as e:
#     #         raise CustomException(e, sys)
    
#     import os

#     def load_data(self, file_path):
#         try:
#             # anchor to project root
#             project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
#             full_path = os.path.join(project_root, file_path)

#             full_path = os.path.abspath(full_path)

#             print("Resolved path:", full_path)

#             df = self.spark.read.text(full_path)
#             return df

#         except Exception as e:
#             raise CustomException(e, sys)