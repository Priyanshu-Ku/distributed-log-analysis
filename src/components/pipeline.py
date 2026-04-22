import sys

from src.components.data_ingestion import DataIngestion
from src.components.data_preprocessing import LogPreprocessor
from src.components.data_transformation import DataTransformation
from src.components.analysis import LogAnalysis

from src.utils.logger import logging
from src.utils.exception import CustomException


class LogPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def run_pipeline(self):
        try:
            logging.info("Pipeline execution started")

            # =========================
            # Step 1: Data Ingestion
            # =========================
            ingestion = DataIngestion()
            spark = ingestion.start_spark_session()

            logging.info("Loading raw data")
            raw_df = ingestion.load_data(self.file_path)

            # =========================
            # Step 2: Preprocessing
            # =========================
            logging.info("Parsing logs")
            preprocessor = LogPreprocessor()
            parsed_df = preprocessor.parse_logs(raw_df)

            # =========================
            # Step 3: Transformation
            # =========================
            logging.info("Transforming data")
            transformer = DataTransformation()
            clean_df = transformer.transform(parsed_df)

            # Optional optimization
            clean_df.cache()
            clean_df.count()

            # =========================
            # Step 4: Analysis
            # =========================
            logging.info("Running analysis")
            analyzer = LogAnalysis()

            results = {
                "log_distribution": analyzer.log_level_distribution(clean_df),
                "top_components": analyzer.top_components(clean_df),
                "warn_components": analyzer.warn_components(clean_df),
                "hourly_analysis": analyzer.hourly_analysis(clean_df),
                "warn_spikes": analyzer.warn_spike_analysis(clean_df),
                "top_failure_messages": analyzer.top_failure_messages(clean_df),
                "process_failures": analyzer.process_failure_analysis(clean_df),
                "failure_risk": analyzer.failure_risk_analysis(clean_df),
                "component_risk": analyzer.component_risk_analysis(clean_df)
            }

            logging.info("Pipeline execution completed successfully")

            return results

        except Exception as e:
            logging.error("Pipeline execution failed")
            raise CustomException(e, sys)