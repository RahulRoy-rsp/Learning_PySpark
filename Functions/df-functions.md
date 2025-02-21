# PySpark Window Functions

Window functions allow you to perform calculations across a set of table rows that are related to the current row. They are particularly useful for performing operations like ranking, cumulative sums, and moving averages.

---

## 1. Row Number (`row_number()`)

**Syntax:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("column_name").orderBy("numeric_column")
df.withColumn("row_number", F.row_number().over(windowSpec))
```

**Example:**
```python
df.withColumn("row_number", F.row_number().over(Window.partitionBy("role").orderBy("runs_scored"))).show()
```

**Explanation:**  
- This function assigns a unique row number to each row within a partition (e.g., for each `role`).
- Rows are ordered based on `runs_scored`. The row number will restart for each partition.

---

## 2. Rank (`rank()`)

**Syntax:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("column_name").orderBy("numeric_column")
df.withColumn("rank", F.rank().over(windowSpec))
```

**Example:**
```python
df.withColumn("rank", F.rank().over(Window.partitionBy("role").orderBy("runs_scored"))).show()
```

**Explanation:**  
- This assigns a rank to each row in the partition, with equal values receiving the same rank, but leaving gaps for duplicate values.
- For example, if two players share the highest score, they both will get rank 1, and the next player will get rank 3.

---

## 3. Dense Rank (`dense_rank()`)

**Syntax:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("column_name").orderBy("numeric_column")
df.withColumn("dense_rank", F.dense_rank().over(windowSpec))
```

**Example:**
```python
df.withColumn("dense_rank", F.dense_rank().over(Window.partitionBy("role").orderBy("runs_scored"))).show()
```

**Explanation:**  
- Similar to `rank()`, but there are no gaps in the rank sequence. If two players have the same highest score, both will get rank 1, but the next player will receive rank 2.
  
---

## 4. Cumulative Sum (`sum()` with `Window`)

**Syntax:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("column_name").orderBy("numeric_column").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("cumulative_sum", F.sum("numeric_column").over(windowSpec))
```

**Example:**
```python
df.withColumn("cumulative_sum", F.sum("runs_scored").over(Window.partitionBy("role").orderBy("match_date").rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
```

**Explanation:**  
- This calculates the cumulative sum of `runs_scored` for each role ordered by `match_date`.
- It adds up the scores for each row in sequence, starting from the first row and progressing down.

---

## 5. Moving Average (`avg()` with `Window`)

**Syntax:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("column_name").orderBy("numeric_column").rowsBetween(-n, n)
df.withColumn("moving_avg", F.avg("numeric_column").over(windowSpec))
```

**Example:**
```python
df.withColumn("moving_avg", F.avg("runs_scored").over(Window.partitionBy("role").orderBy("match_date").rowsBetween(-2, 2))).show()
```

**Explanation:**  
- This calculates a moving average for `runs_scored` considering the previous and next 2 matches (`-2, 2`).
- This is useful for trend analysis, where the average is calculated over a sliding window of values.

---

# PySpark User-Defined Functions (UDFs)

User-Defined Functions (UDFs) allow you to write custom transformations on DataFrame columns in PySpark.

---

## 1. Basic UDF Creation

**Syntax:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def custom_function(value):
    return value * 2

# Register UDF
custom_udf = udf(custom_function, IntegerType())

df.withColumn("double_value", custom_udf("numeric_column")).show()
```

**Example:**
```python
def custom_function(value):
    if value > 50:
        return "high"
    else:
        return "low"

# Register UDF
custom_udf = udf(custom_function, StringType())

df.withColumn("performance_level", custom_udf("runs_scored")).show()
```

**Explanation:**  
- In this example, the UDF `custom_function` checks whether `runs_scored` is above 50 and classifies it as "high" or "low".
- UDFs allow you to apply custom logic to each row of your DataFrame.

---

## 2. UDF with Multiple Columns

**Syntax:**
```python
def custom_function(col1, col2):
    return col1 + col2

# Register UDF
custom_udf = udf(custom_function, IntegerType())

df.withColumn("sum_col", custom_udf("col1", "col2")).show()
```

**Example:**
```python
def compare_scores(score1, score2):
    if score1 > score2:
        return "score1"
    elif score2 > score1:
        return "score2"
    else:
        return "tie"

# Register UDF
compare_udf = udf(compare_scores, StringType())

df.withColumn("top_score", compare_udf("score1", "score2")).show()
```

**Explanation:**  
- This UDF compares two columns, `score1` and `score2`, and returns which score is higher, or if it's a tie.
- UDFs can take multiple columns as input, which is useful for complex row-wise operations.

---

## 3. UDF with Date Columns

**Syntax:**
```python
from datetime import datetime

def custom_date_function(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.year

# Register UDF
date_udf = udf(custom_date_function, IntegerType())

df.withColumn("year", date_udf("date_column")).show()
```

**Example:**
```python
def calculate_age(dob):
    current_year = datetime.now().year
    return current_year - dob.year

# Register UDF
age_udf = udf(calculate_age, IntegerType())

df.withColumn("age", age_udf("date_of_birth")).show()
```

**Explanation:**  
- This UDF takes a `date_of_birth` column, calculates the age by subtracting the year from the current year, and adds a new `age` column.

---

## 4. UDF for String Manipulation

**Syntax:**
```python
def string_manipulation_function(input_str):
    return input_str.lower()

# Register UDF
string_udf = udf(string_manipulation_function, StringType())

df.withColumn("lowercase_name", string_udf("name")).show()
```

**Example:**
```python
def concatenate_name_and_role(first_name, last_name, role):
    return f"{first_name} {last_name} ({role})"

# Register UDF
concatenate_udf = udf(concatenate_name_and_role, StringType())

df.withColumn("full_name_role", concatenate_udf("first_name", "last_name", "role")).show()
```

**Explanation:**  
- This UDF takes multiple string columns (first name, last name, and role) and combines them into a single formatted string.

---

> **Note:**  
> These are just a few more advanced PySpark functions that are commonly used.🚀



