import sys

from src.components.data_ingestion import DataIngestion
from src.components.data_preprocessing import LogPreprocessor
from src.components.data_transformation import DataTransformation
from src.components.analysis import LogAnalysis
from src.components.model_training import LogModel

from src.utils.logger import logging
from src.utils.exception import CustomException


class LogPipeline:
    def __init__(self, file_path):
        self.file_path = file_path

    def run_pipeline(self):
        try:
            logging.info("Pipeline started")

            # Step 1: Ingestion
            ingestion = DataIngestion()
            ingestion.start_spark_session()
            raw_df = ingestion.load_data(self.file_path)

            # Step 2: Preprocessing
            preprocessor = LogPreprocessor()
            parsed_df = preprocessor.parse_logs(raw_df)

            # Step 3: Transformation
            transformer = DataTransformation()
            clean_df = transformer.transform(parsed_df)

            clean_df.cache()
            clean_df.count()

            # Step 4: Analysis
            analyzer = LogAnalysis()

            results = {
                "log_distribution": analyzer.log_level_distribution(clean_df),
                "top_components": analyzer.top_components(clean_df),
                "warn_components": analyzer.warn_components(clean_df),
                "hourly_analysis": analyzer.hourly_analysis(clean_df),
                "failure_risk": analyzer.failure_risk_analysis(clean_df),
                "component_risk": analyzer.component_risk_analysis(clean_df)
            }

            # Step 5: Model
            model = LogModel()

            features = model.prepare_features(clean_df)
            model.train_model(features)
            predictions = model.predict(features)

            evaluation = model.evaluate(predictions)
            model.visualize(predictions)

            results["model_predictions"] = predictions
            results["evaluation"] = evaluation

            logging.info("Pipeline completed")

            return results

        except Exception as e:
            logging.error("Pipeline failed")
            raise CustomException(e, sys)