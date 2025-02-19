# [Solution 1](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/df-exercise.md#exercise-1-create-a-simple-dataframe)
```python
## Solution 1: Create a Simple DataFrame
from pyspark.sql import SparkSession

# Initializing the Spark session
spark = SparkSession.builder.appName("DataFrame Exercise-1").getOrCreate()

# Creating the DataFrame
data = [
  ("Alice", "Capsey", "Batter"),
  ("Steve", "Smith", "Batter"),
  ("Darcie", "Brown", "Bowler"),
  ("Sean", "Williams", "All-Rounder"),
  ("Joel", "Davis", "Bowler"),
]

columns = ["first_name", "last_name", "role"]
df = spark.createDataFrame(data, columns)

# Displaying the DataFrame
df.show()
```

### OUTPUT
![image](https://github.com/user-attachments/assets/fe32fea5-11fc-41e8-8e55-bf8347908079)

---

# [Solution 2](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/df-exercise.md#exercise-2-create-a-dataframe-with-specific-data-types)
```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Defining the schema
schema = StructType([
    StructField("device", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True)
])

# Creating the DataFrame
data = [
    ("Laptop", 1200.50, 5),
    ("Tablet", 450.75, 10),
    ("Smartphone", 899.99, 15),
    ("Monitor", 299.49, 7),
    ("Headphones", 99.99, 20)
]

df = spark.createDataFrame(data, schema)

# Displaying the DataFrame
df.show()
```

### OUTPUT
![image](https://github.com/user-attachments/assets/a9934ade-90a8-40a1-a30e-d3ee9b608377)

---

# [Solution 3](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/df-exercise.md#exercise-3-read-a-csv-file-into-a-dataframe)
```python
# Reading the CSV file
df = spark.read.csv("csv-files/df-exercise.csv", header=True, inferSchema=True)

# Display DataFrame
df.show()

```
### OUTPUT
![image](https://github.com/user-attachments/assets/1b8f2cec-e901-4442-939b-2d8ec85c8850)

---

| References |
| ---------- |
**[Creating Dataframes](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/dataframes.md#creating-dataframes-in-pyspark)**
**[Questions](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/df-exercise.md)**
**[Download CSV](https://github.com/RahulRoy-rsp/Learning_PySpark/tree/main/Dataframes/csv-files)**

---
**[Back to Home Page](https://github.com/RahulRoy-rsp/Learning_PySpark)**
