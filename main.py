from src.components.pipeline import LogPipeline
import os

if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(BASE_DIR, "data", "raw", "HDFS.log")

    pipeline = LogPipeline(file_path)
    results = pipeline.run_pipeline()

    print("\n=== Top Components ===")
    results["top_components"].show(10, truncate=False)

    print("\n=== Failure Risk ===")
    results["failure_risk"].show()

    print("\n=== Component Risk ===")
    results["component_risk"].show(10, truncate=False)


# import os

# from src.components.data_ingestion import DataIngestion
# from src.components.data_preprocessing import LogPreprocessor
# from src.components.data_transformation import DataTransformation

# if __name__ == "__main__":
    
#     BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#     file_path = os.path.join(BASE_DIR, "data", "raw", "HDFS.log")

#     ingestion = DataIngestion()
#     ingestion.start_spark_session()

#     df = ingestion.load_data(file_path)

#     df.show(5, truncate=False)
    
#     preprocessor = LogPreprocessor()
#     parsed_df = preprocessor.parse_logs(df)

#     parsed_df.show(5, truncate=False)
#     parsed_df.printSchema()

#     transformer = DataTransformation()
#     clean_df = transformer.transform(parsed_df)

#     clean_df.show(5, truncate=False)
#     clean_df.printSchema()