
# Creating DataFrames in PySpark

PySpark provides several ways to create DataFrames. Below are some common methods to create a DataFrame in PySpark.

## 1. Creating a DataFrame from a List of Tuples

You can create a DataFrame from a list of tuples using the `createDataFrame` method. Each tuple represents a row in the DataFrame.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("dfs-1").getOrCreate()

# List of tuples
data = [("Alice", 21), ("Natie", 35), ("Sophie", 25)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.show()
```

## 2. Creating a DataFrame from a List of Dictionaries

You can also create a DataFrame from a list of dictionaries. Each dictionary represents a row in the DataFrame.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("dfs-2").getOrCreate()

# List of dictionaries
data = [{"Name": "Alice", "Age": 21}, {"Name": "Natie", "Age": 35}, {"Name": "Sophie", "Age": 25}]

# Create DataFrame
df = spark.createDataFrame(data)

# Show DataFrame
df.show()
```

## 3. Creating a DataFrame from a Pandas DataFrame

If you have a Pandas DataFrame, you can easily convert it to a PySpark DataFrame.

```python
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("dfs-3").getOrCreate()

# Create a Pandas DataFrame
pandas_df = pd.DataFrame({
    "Name": ["Alice", "Natie", "Sophie"],
    "Age": [21, 35, 25]
})

# Convert to PySpark DataFrame
df = spark.createDataFrame(pandas_df)

# Show DataFrame
df.show()
```

## 4. Creating a DataFrame from a CSV File

You can create a DataFrame by reading data from a CSV file using the `read.csv` method.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("dfs-4").getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("path/players.csv", header=True, inferSchema=True)

# Show DataFrame
df.show()
```


## 5. Creating a DataFrame from an RDD

You can create a DataFrame from an RDD (Resilient Distributed Dataset) by converting the RDD to a DataFrame.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("dfs-5").getOrCreate()

# Create an RDD
rdd = spark.sparkContext.parallelize([("Alice", 21), ("Natie", 35), ("Sophie", 25)])

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Convert RDD to DataFrame
df = spark.createDataFrame(rdd, schema)

# Show DataFrame
df.show()
```
