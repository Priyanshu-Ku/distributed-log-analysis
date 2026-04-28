You are a Senior Big Data Engineer and ML Pipeline Maintainer.

Your task is to FIX and HARDEN this distributed log analysis system.

This is NOT a review task. You MUST:

- Modify code
- Validate fixes
- Re-run pipeline
- Confirm outputs

Work STRICTLY step-by-step.

---

## 🚨 STRICT EXECUTION RULES

- DO NOT skip steps
- DO NOT assume correctness
- DO NOT summarize without execution
- MODIFY files where required
- SHOW exact code patches
- VALIDATE after each step
- If something fails → DEBUG before moving forward

---

## PHASE 1 — FIX PIPELINE RETURN BUG (CRITICAL)

Problem:
main.py crashes with:
TypeError: 'NoneType' object is not subscriptable

Root cause:
pipeline.py → run_pipeline() does NOT return results

---

Step 1.1:
Open:
src/components/pipeline.py

Step 1.2:
Locate end of run_pipeline()

Step 1.3:
ADD:

results["clean_df"] = clean_df
return results

Step 1.4:
Ensure results dictionary contains:

- log_distribution
- top_components
- warn_components
- hourly_analysis
- failure_risk
- component_risk
- model_predictions
- evaluation
- clean_df

---

Step 1.5: VALIDATE

Run:
python main.py

Expected:

- No crash
- Tables printed successfully

---

## PHASE 2 — FIX DATA QUALITY (DEDUPLICATION)

Problem:
2118 duplicate rows exist in transformed dataset

Fix required in:
src/components/data_transformation.py

---

Step 2.1:
Open transformation file

Step 2.2:
After datetime creation and filtering, ADD:

clean_df = clean_df.dropDuplicates(
["date", "time", "process_id", "log_level", "component", "message"]
)

Reason:
dropDuplicates removes duplicate rows based on selected columns :contentReference[oaicite:0]{index=0}

---

Step 2.3: VALIDATE

Add temporary check:

print("Before:", parsed_df.count())
print("After:", clean_df.count())

Expected:
After < Before

---

## PHASE 3 — FIX MODEL QUALITY (SCALING + FEATURE ALIGNMENT)

Problem:
Model biased toward event_count (high magnitude feature)

---

Step 3.1:
Open:
src/components/model_training.py

Step 3.2:
Ensure StandardScaler is used:

from sklearn.preprocessing import StandardScaler

Step 3.3:
Inside **init**:

self.scaler = StandardScaler()

---

Step 3.4:
Modify train_model():

X = train_df[self.feature_cols].astype("float32")
X_scaled = self.scaler.fit_transform(X)

self.model.fit(X_scaled)

---

Step 3.5:
Modify predict():

X_scaled = self.scaler.transform(X_test)

---

Step 3.6: VALIDATE

Run:
python main.py

Check:

- anomaly distribution changes
- not only high event_count anomalies

---

## PHASE 4 — FIX TRAIN-TEST SPLIT (DATA LEAKAGE)

Problem:
Model was trained + tested on same data

---

Step 4.1:
Add function:

def train_test_split(self, pdf, split_ratio=0.7):
split_idx = int(len(pdf) \* split_ratio)
return pdf.iloc[:split_idx], pdf.iloc[split_idx:]

---

Step 4.2:
Modify pipeline.py:

Replace:

model.train_model(features)
predictions = model.predict(features)

With:

train_df, test_df = model.train_test_split(features)
model.train_model(train_df)
predictions = model.predict(test_df)

---

Step 4.3: VALIDATE

Check:

- Train size ≠ Test size
- No overlap

---

## PHASE 5 — FIX PERFORMANCE ISSUES

Problem:
Spark memory spill warnings due to caching

---

Step 5.1:
In pipeline.py, MODIFY:

OPTION A (recommended):
Remove:
clean_df.cache()

OPTION B:
Use:
clean_df.persist()

---

Step 5.2:
After model usage:

clean_df.unpersist()

---

Step 5.3: VALIDATE

Run:
python main.py

Check:

- fewer memory warnings

---

## PHASE 6 — FIX UNSAFE toPandas()

Problem:
toPandas() may crash on large data

---

Step 6.1:
Search in project for:
.toPandas()

Step 6.2:
Ensure:

- ONLY used on aggregated data
- NEVER used on full dataset

Step 6.3:
If large DF → apply:

.limit(1000)

---

Step 6.4: VALIDATE

Ensure no large DataFrame converted to pandas

---

## PHASE 7 — FIX VISUALIZATION OUTPUT

Problem:
Plots not saved

---

Step 7.1:
Modify all plt.show() calls:

Replace with:

plt.savefig("outputs/figure_name.png")

---

Step 7.2:
Ensure directory exists:

os.makedirs("outputs", exist_ok=True)

---

Step 7.3: VALIDATE

Check:
outputs/ contains images

---

## PHASE 8 — CLEAN REQUIREMENTS

Step 8.1:
Open requirements.txt

Step 8.2:
Remove:

- duplicate packages
- inconsistent versions

---

Step 8.3:
Run:

pip check

Fix dependency conflicts

---

## PHASE 9 — FINAL SYSTEM VALIDATION

Run full pipeline:

python main.py

---

Verify:

✔ No crashes  
✔ Outputs printed  
✔ Plots saved  
✔ Model evaluation printed  
✔ No major warnings

---

## PHASE 10 — FINAL REPORT

Output:

1. Files modified
2. Bugs fixed
3. Performance improvements
4. Final evaluation metrics
5. Remaining limitations

---

## END

## GOALS ACHIEVED (EXECUTION END MARKER)

- Fixed pipeline return/result propagation (including clean_df in results)
- Added deduplication in transformation stage
- Enforced train/test split usage in orchestration
- Removed eager cache pressure path and added unpersist lifecycle handling
- Limited unsafe toPandas conversion in anomaly plotting
- Replaced interactive plot rendering with persisted PNG outputs
- Cleaned requirements.txt duplicates/inconsistencies
- Resolved pip dependency conflicts and verified with pip check
- Validated full pipeline: no crash, outputs printed, evaluation produced, plots saved

---
