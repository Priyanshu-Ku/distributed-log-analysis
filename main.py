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
    results["component_risk"].show()

    print("\n=== Evaluation ===")
    print(results["evaluation"])