from src.components.pipeline import LogPipeline
from src.components.analysis import LogAnalysis
import os


if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(BASE_DIR, "data", "raw", "HDFS.log")

    # 🔥 Run pipeline
    pipeline = LogPipeline(file_path)
    results = pipeline.run_pipeline()

    # 🔥 Print outputs (existing)
    print("\n=== Top Components ===")
    results["top_components"].show(10, truncate=False)

    print("\n=== Failure Risk ===")
    results["failure_risk"].show()

    print("\n=== Component Risk ===")
    results["component_risk"].show()

    print("\n=== Evaluation ===")
    print(results["evaluation"])

    # ============================================================
    # 🔥 NEW: Visualization + Insights Layer
    # ============================================================

    print("\n=== Generating Visual Insights ===")

    analysis = LogAnalysis()

    # ⚠️ IMPORTANT: We need the clean dataframe
    # Assuming pipeline returns it (if not, fix pipeline — see below)
    clean_df = results.get("clean_df", None)

    if clean_df is not None:
        # 🔥 Visualizations
        analysis.plot_top_components(clean_df)
        analysis.plot_hourly_distribution(clean_df)
        analysis.plot_warning_components(clean_df)
        analysis.plot_failure_trend(clean_df)

    else:
        print("⚠️ clean_df not found in results. Please update pipeline to return it.")