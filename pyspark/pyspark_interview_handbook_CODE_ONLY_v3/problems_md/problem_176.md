# Problem 176: 176 - MLlib Basics: Vectorassembler challenge

**Category:** MLlib Basics

## Problem
Index -> Vectorize -> LogisticRegression pipeline.

### Input DataFrame
Name: `sessions`

Schema:
```
root
 |-- uid: string
 |-- name: string
 |-- email: string
 |-- country: string
 |-- signup_ts: timestamp
```

## Solution (PySpark)
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
label_indexer = StringIndexer(inputCol="event_type", outputCol="label", handleInvalid="skip")
assembler = VectorAssembler(inputCols=["value"], outputCol="features")
lr = LogisticRegression(maxIter=10)
model = Pipeline(stages=[label_indexer, assembler, lr]).fit(sessions)
res = model.transform(sessions)
```

## Variations
- train/test split.
- RandomForest alternative.
- StandardScaler step.

---
