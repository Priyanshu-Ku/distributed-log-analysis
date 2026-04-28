You are a Senior Big Data Engineer.

Your task is to UPGRADE FEATURE ENGINEERING + MODEL LOGIC
to fix weak anomaly detection (currently biased toward event_count).

STRICT MODE:
- Do NOT skip steps
- Show code patches
- Validate after each phase
- Stop if failure occurs

--------------------------------------------------
PHASE 1 — UPGRADE FEATURE ENGINEERING
--------------------------------------------------

Target file:
src/components/feature_builder.py

Step 1.1:
Add imports at top:

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, avg

--------------------------------------------------

Step 1.2:
Modify feature builder AFTER aggregation

ADD new features:

window_spec = Window.orderBy("bucket_start")

features_df = features_df.withColumn(
    "event_delta",
    col("event_count") - lag("event_count", 1).over(window_spec)
)

features_df = features_df.withColumn(
    "rolling_mean",
    avg("event_count").over(window_spec.rowsBetween(-3, 0))
)

features_df = features_df.withColumn(
    "warn_intensity",
    col("warn_count") / (col("unique_processes") + 1)
)

--------------------------------------------------

Step 1.3:
Handle null values:

features_df = features_df.fillna(0)

--------------------------------------------------

Step 1.4:
REMOVE redundant feature:

DELETE:
info_count

From:
- aggregation
- select
- return

--------------------------------------------------

VALIDATE:

Run:
python main.py

Check:
- No crash
- Features include new columns:
  event_delta, rolling_mean, warn_intensity

--------------------------------------------------
PHASE 2 — UPDATE MODEL FEATURE LIST
--------------------------------------------------

Target file:
src/components/model_training.py

Step 2.1:
Update feature_cols:

REPLACE:

self.feature_cols = [
    "event_count",
    "warn_count",
    "info_count",
    "unique_components",
    "unique_processes",
    "warn_ratio"
]

WITH:

self.feature_cols = [
    "event_count",
    "warn_count",
    "unique_components",
    "unique_processes",
    "warn_ratio",
    "event_delta",
    "rolling_mean",
    "warn_intensity"
]

--------------------------------------------------

VALIDATE:

Run:
python main.py

Check:
- No KeyError
- Model trains successfully

--------------------------------------------------
PHASE 3 — IMPROVE ANOMALY DETECTION (THRESHOLD BASED)
--------------------------------------------------

Target file:
src/components/model_training.py

Step 3.1:
Modify predict()

REPLACE prediction logic:

preds = self.model.predict(X_scaled)

test_df["anomaly"] = np.where(preds == -1, "ANOMALY", "NORMAL")

WITH:

scores = self.model.decision_function(X_scaled)

threshold = np.percentile(scores, 10)

test_df["anomaly_score"] = scores

test_df["anomaly"] = np.where(scores < threshold, "ANOMALY", "NORMAL")

--------------------------------------------------

VALIDATE:

Run:
python main.py

Check:
- anomaly_score column present
- anomalies detected based on threshold

--------------------------------------------------
PHASE 4 — ADD RULE-BASED FAILURE SIGNAL
--------------------------------------------------

Target file:
src/components/model_training.py

Step 4.1:
Add rule-based anomaly:

test_df["rule_flag"] = np.where(
    test_df["warn_ratio"] > 0.2, 1, 0
)

test_df["final_anomaly"] = np.where(
    (test_df["anomaly"] == "ANOMALY") | (test_df["rule_flag"] == 1),
    "ANOMALY",
    "NORMAL"
)

--------------------------------------------------

VALIDATE:

Run:
python main.py

Check:
- final_anomaly column exists
- anomalies include WARN-heavy windows

--------------------------------------------------
PHASE 5 — INCREASE DATA GRANULARITY
--------------------------------------------------

Target file:
src/components/feature_builder.py

Step 5.1:
Change window size

FIND:

window_size="5 minutes"

REPLACE WITH:

window_size="1 minute"

--------------------------------------------------

VALIDATE:

Run:
python main.py

Check:
- Number of rows increased (>100)
- Model still runs

--------------------------------------------------
PHASE 6 — FINAL VALIDATION
--------------------------------------------------

Run:

python main.py

--------------------------------------------------

VERIFY:

✔ No crash  
✔ Evaluation printed  
✔ Plots saved  
✔ More anomalies detected  
✔ Anomalies align with spikes  

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

Return:

1. Files modified  
2. Code patches  
3. New feature list  
4. Final anomaly count  
5. Improvement summary  

END