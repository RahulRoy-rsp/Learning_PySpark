
# PySpark DataFrame Operations

## 1. Selecting specific columns (`select()`)

**Syntax:**
```python
df.select("column1", "column2", ...)
```

**Example:**
```python
df.select("first_name", "role").show()
```

**Explanation:**
- Extracts specific columns from a DataFrame.
- In the above example, it will extract only `"first_name", "role"` from the dataframe named `df`.

---

## 2. Adding a Column (`withColumn()`)

**Syntax:**
```python
df.withColumn("new_column", expression)
```

**Example:**
```python
from pyspark.sql.functions import lit

df = df.withColumn("team", lit("Australia"))
```

**Explanation:**  
- Creates a new column with a specified value or expression.
- In the above example, a new column `team` is added to the dataframe `df` with its value specified as `Australia`.
- We could also use the other columns value to assign it to the newly added column.

---

## 3. Dropping a Column (`drop()`)

**Syntax:**
```python
df.drop("column_name")
```

**Example:**
```python
df = df.drop("last_name")
```

**Explanation:**  
- Removes a specified column from the DataFrame.
- In the above example, the column named `last_name` is removed from the dataframe `df`.

---

## 4. Filtering Data (`filter()` / `where()`)

**Syntax:**
```python
df.filter(condition)
OR
df.where(condition)
```

**Example:**
```python
df.filter(df.role == "Batter").show()
```

**Explanation:**  
- Filters rows that satisfy the specified condition.
- In the above example, we are only selecting the records wherein the column **`role`** has value **`Batter`**
---

## 5. Sorting Data (`orderBy()`)

**Syntax:**
```python
df.orderBy(df.column_name.desc())
```

**Example:**
```python
df.orderBy(df.first_name.asc()).show() # for ascending
df.orderBy(df.first_name.desc()).show() # for descending
```

**Explanation:**  
- Sorts the DataFrame in ascending or descending order.

---

## 6. Grouping and Aggregations (`groupBy()`)

**Syntax:**
```python
df.groupBy("column_name").agg(aggregation_function)
```

**Example:**
```python
from pyspark.sql.functions import count

df.groupBy("role").agg(count("*")).show()
```

**Explanation:**  
- Groups data based on a column and applies an aggregation function.
- In the above example, we are selecting the count of a particular `role` in the dataframe `df`.

---

## 7. Joining DataFrames (`join()`)

**Syntax:**
```python
df1.join(df2, df1.key_column == df2.key_column, "join_type")
```

**Example:**
```python
df1.join(df2, df1.player_id == df2.player_id, "inner").show()
```

**Explanation:**
- Combines two DataFrames based on a common column.
- In the above example, we are joining the two dataframes with the common key as `player_id`

---

## 8. Count of the dataframe (`count()`)

**Syntax:**
```python
df.count()
```

**Example:**
```python
df.count()
```

**Explanation:**
- Shows the number of rows in the dataframe.

---

## 9. Columns in the dataframe (`columns`)

**Syntax:**
```python
df.columns
```

**Example:**
```python
df.columns
```

**Explanation:**
- Shows the columns available in the dataframe.

---

## 10. Schema of the dataframe (`printSchema()`)

**Syntax:**
```python
df.printSchema()
```

**Example:**
```python
df.printSchema()
```

**Explanation:**
- Shows the schema of the dataframe, typically shows the column name with its data type.

---


> **Note:**  
> These are just a few commonly used DataFrame operations in PySpark. Each method has additional parameters and alternative implementations to suit different use cases. PySpark provides many more powerful methods for data transformation and analysis. 🚀  
