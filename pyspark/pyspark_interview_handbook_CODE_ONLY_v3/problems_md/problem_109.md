# Problem 109: 109 - MLlib Basics: Logisticregression challenge

**Category:** MLlib Basics

## Problem
Index -> Vectorize -> LogisticRegression pipeline.

### Input DataFrame
Name: `orders`

Schema:
```
root
 |-- order_id: string
 |-- product_id: string
 |-- price: double
 |-- quantity: int
 |-- order_ts: timestamp
 |-- tags: array<string>
```

## Solution (PySpark)
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
label_indexer = StringIndexer(inputCol="event_type", outputCol="label", handleInvalid="skip")
assembler = VectorAssembler(inputCols=["value"], outputCol="features")
lr = LogisticRegression(maxIter=10)
model = Pipeline(stages=[label_indexer, assembler, lr]).fit(orders)
res = model.transform(orders)
```

## Variations
- train/test split.
- RandomForest alternative.
- StandardScaler step.

---
