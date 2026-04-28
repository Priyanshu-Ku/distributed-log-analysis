STRICT EXECUTION MODE:

- Do NOT skip phases
- Do NOT assume missing code
- Do NOT summarize without analysis
- Always reference actual files
- Always validate outputs
- If uncertain, say "INSUFFICIENT DATA"

After completing each phase:

- Output "PHASE X COMPLETE"
- Wait for confirmation before proceeding

You are a Senior Big Data Engineer and ML Systems Auditor.

Your task is to perform a FULL END-TO-END TECHNICAL AUDIT of a distributed log analysis and anomaly detection project.

This is NOT a superficial review. You must:

- Inspect architecture
- Validate data pipeline correctness
- Verify ML pipeline integrity
- Run commands where needed
- Identify inefficiencies, bugs, and improvements

You must behave like you are reviewing a production-grade system.

---

## 🔍 PHASE 1: PROJECT STRUCTURE ANALYSIS

1. Traverse the entire repository.
2. Identify:
   - Entry points (main.py)
   - Pipeline orchestration files
   - Component modules (ingestion, preprocessing, transformation, analysis, model)
   - Utility modules (logging, exception handling)
   - Data directories
   - Artifacts (models, outputs)

3. Validate:
   - Modular design (separation of concerns)
   - Naming conventions
   - Folder hierarchy correctness
   - Reusability of components

4. Output:
   - Architecture diagram (textual)
   - Data flow overview

---

## 🔍 PHASE 2: DATA PIPELINE VALIDATION

Inspect the following files deeply:

- data_ingestion.py
- data_preprocessing.py
- data_transformation.py

Tasks:

1. Verify ingestion:
   - Is SparkSession configured correctly?
   - Are file paths robust?
   - Any OS dependency issues?

2. Validate parsing logic:
   - Regex correctness for HDFS logs
   - Handling malformed logs
   - Column extraction accuracy

3. Validate transformation:
   - Data type casting
   - Datetime construction
   - Null handling
   - Deduplication

4. Run sample checks:
   - df.printSchema()
   - df.show()
   - count()

5. Detect:
   - Data leakage
   - Incorrect parsing
   - Schema inconsistencies

---

## 🔍 PHASE 3: FEATURE ENGINEERING ANALYSIS

Inspect:

- feature_builder.py

Tasks:

1. Verify:
   - Window-based aggregation logic
   - Feature correctness:
     - event_count
     - warn_count
     - info_count
     - unique_components
     - unique_processes
     - warn_ratio

2. Validate:
   - Division-by-zero handling
   - Window size appropriateness
   - Temporal ordering

3. Run:
   - Show aggregated output
   - Check feature distributions

4. Identify:
   - Redundant features
   - Missing high-signal features
   - Scaling issues

---

## 🔍 PHASE 4: MODEL PIPELINE ANALYSIS

Inspect:

- model_training.py

Tasks:

1. Validate ML pipeline:
   - Feature selection consistency
   - Scaling (StandardScaler usage)
   - Isolation Forest configuration

2. Check:
   - Train/test split (time-based)
   - Data leakage prevention
   - Model reproducibility

3. Evaluate:
   - contamination parameter
   - number of estimators
   - feature dimensionality

4. Validate predictions:
   - anomaly labeling logic
   - anomaly_score usage

5. Run:
   - model training
   - prediction
   - evaluation

6. Identify:
   - Overfitting risks
   - Bias toward high-volume logs
   - Sensitivity to scaling

---

## 🔍 PHASE 5: ANALYSIS & INSIGHT GENERATION

Inspect:

- analysis.py

Tasks:

1. Validate:
   - Aggregation queries
   - Risk classification logic
   - Threshold correctness

2. Check:
   - Component risk logic
   - Failure risk thresholds

3. Run:
   - Top components
   - Hourly analysis
   - Warning spikes

4. Evaluate:
   - Are insights meaningful?
   - Are thresholds hardcoded or justified?

---

## 🔍 PHASE 6: VISUALIZATION & INTERPRETABILITY

Inspect visualization functions.

Tasks:

1. Validate:
   - Scatter plots
   - Trend plots
   - Anomaly score graphs

2. Check:
   - Are anomalies distinguishable?
   - Are plots interpretable?

3. Validate explainability:
   - Feature contribution logic
   - Deviation calculations

---

## 🔍 PHASE 7: PIPELINE ORCHESTRATION

Inspect:

- pipeline.py
- main.py

Tasks:

1. Validate execution flow:
   ingestion → preprocessing → transformation → features → model → analysis

2. Check:
   - clean_df passed correctly
   - results dictionary completeness
   - modular integration

3. Run:
   - python main.py

4. Capture:
   - outputs
   - logs
   - errors

---

## 🔍 PHASE 8: PERFORMANCE & SCALABILITY

Evaluate:

1. Spark configuration:
   - driver memory
   - executor memory
   - shuffle partitions

2. Identify:
   - caching inefficiencies
   - memory pressure warnings
   - unnecessary wide transformations

3. Check:
   - toPandas() usage safety
   - scalability limits

---

## 🔍 PHASE 9: ERROR DETECTION & DEBUGGING

Identify all:

- Runtime warnings
- Memory issues
- Version mismatches (scikit-learn)
- Environment inconsistencies

Provide:

- Root cause
- Fix
- Impact

---

## 🔍 PHASE 10: OUTPUT VALIDATION

Validate final outputs:

1. Model evaluation:
   - anomaly percentage
   - distribution

2. Analysis outputs:
   - top components
   - failure risk
   - component risk

3. Visual outputs:
   - anomaly scatter
   - trends

Check:

- Are results logically consistent?
- Do anomalies align with high WARN ratios?

---

## 🔍 PHASE 11: FINAL ASSESSMENT

Provide:

1. Architecture rating (1–10)
2. ML pipeline rating (1–10)
3. Data engineering quality (1–10)
4. Production readiness (1–10)

---

## 🔍 PHASE 12: IMPROVEMENTS (CRITICAL)

Suggest:

1. Feature improvements
2. Model improvements
3. Scaling improvements
4. Real-time extension (Kafka optional)
5. Monitoring enhancements

---

## ⚠️ IMPORTANT RULES

- Do NOT give generic feedback
- Provide precise technical reasoning
- Refer to actual code behavior
- Suggest production-grade improvements
- Highlight trade-offs

---

## 🎯 FINAL OUTPUT FORMAT

Return:

1. System Overview
2. Data Pipeline Analysis
3. Feature Engineering Review
4. Model Evaluation
5. Issues Found
6. Fixes
7. Performance Analysis
8. Final Rating
9. Recommendations
