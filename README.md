# Distributed Log Analysis and Failure Prediction

A PySpark-based big data pipeline that processes large HDFS logs, detects anomalous behavior, and generates operational risk insights with visual outputs.

## Why this project

Distributed systems generate huge volumes of logs that are difficult to inspect manually. This project automates:
1. log parsing and cleaning,
2. time-window feature engineering,
3. anomaly detection with Isolation Forest,
4. risk analysis for components and time periods.

The current dataset in this repo is large-scale for local analysis:
- ~**1.58 GB** raw log file
- ~**11.17 million** log lines

## What it does

### End-to-end pipeline

`Raw logs -> Parse -> Clean -> Aggregate features -> Train model -> Predict anomalies -> Risk analysis -> Plots`

### Main outputs

1. Top components by log volume
2. Failure risk by hour (LOW/MEDIUM/HIGH)
3. Component risk classification
4. Anomaly predictions and anomaly scores
5. Saved visualizations in `outputs/`

## Project structure

```text
.
├── main.py
├── src/
│   ├── components/
│   │   ├── data_ingestion.py
│   │   ├── data_preprocessing.py
│   │   ├── data_transformation.py
│   │   ├── feature_builder.py
│   │   ├── model_training.py
│   │   ├── analysis.py
│   │   └── pipeline.py
│   └── utils/
│       ├── logger.py
│       └── exception.py
├── data/
│   └── raw/HDFS.log
├── outputs/
├── logs/
├── requirements.txt
└── project_overview.md
```

## Core architecture

### 1) Ingestion layer
- Starts Spark session
- Reads raw text logs using `spark.read.text`

### 2) Processing layer
- Regex-based parsing of date/time/process/log level/component/message
- Data cleaning, timestamp creation, deduplication
- 1-minute window feature generation (`event_count`, `warn_ratio`, `event_delta`, etc.)

### 3) Model layer
- Time-based train/test split
- Feature scaling with `StandardScaler`
- Unsupervised anomaly detection using `IsolationForest`
- Rule-assisted anomaly flagging (`warn_ratio` threshold)

### 4) Analysis and serving layer
- Aggregated risk tables
- Visual charts saved to disk
- Console summaries for quick inspection

### 5) Observability layer
- Pipeline stage logs in `logs/`
- Centralized custom exception wrapper

## Setup

## Prerequisites

1. Python 3.10+
2. Java (required by Spark)
3. Enough memory for large log processing

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Generated artifacts

After execution, the project writes:

- **Visuals** in `outputs/`:
  - `anomaly_event_count.png`
  - `anomaly_score_trend.png`
  - `feature_contribution.png`
  - `top_components.png`
  - `hourly_distribution.png`
  - `warning_components.png`
  - `failure_trend.png`
- **Logs** in `logs/`

## Example observed run metrics

From the current implementation and dataset:
- Feature windows: **2322**
- Train/Test split: **1625 / 697**
- Detected anomalies: **70** (**10.04%** of test windows)
- Full local runtime: ~**18 minutes** (environment-dependent)

## Key design decisions

1. **PySpark over pandas-only** for scalable processing.
2. **Batch pipeline** over streaming for simplicity and reproducibility.
3. **Isolation Forest** because labeled anomaly data is not required.
4. **1-minute windows** for finer anomaly granularity.
5. **Model + rule hybrid** for practical anomaly coverage.

## Current limitations

1. Batch-only (no real-time streaming).
2. No scheduler/orchestrator like Airflow.
3. Thresholds are static and may need tuning by environment.
4. Spark performance can degrade at higher scale without tuning.
5. Security hardening (encryption/IAM/auditing) is not production-complete in this repo.

## Roadmap

1. Add streaming ingestion (Kafka + Structured Streaming).
2. Add orchestration and retries.
3. Add model drift and adaptive thresholding.
4. Add API/dashboard serving layer.
5. Improve cost and performance tuning for larger workloads.

## Documentation

- Deep technical walkthrough: [`project_overview.md`](project_overview.md)

---

If you want to productionize this pipeline, start with: scheduler integration, Spark resource tuning, and security controls.
