# Problem 049: 049 - MLlib Basics: Logisticregression challenge

**Category:** MLlib Basics

## Problem
Index -> Vectorize -> LogisticRegression pipeline.

### Input DataFrame
Name: `users`

Schema:
```
root
 |-- txn_id: string
 |-- user_id: string
 |-- amount: double
 |-- currency: string
 |-- ts: timestamp
```

## Solution (PySpark)
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
label_indexer = StringIndexer(inputCol="event_type", outputCol="label", handleInvalid="skip")
assembler = VectorAssembler(inputCols=["value"], outputCol="features")
lr = LogisticRegression(maxIter=10)
model = Pipeline(stages=[label_indexer, assembler, lr]).fit(users)
res = model.transform(users)
```

## Variations
- train/test split.
- RandomForest alternative.
- StandardScaler step.

---
