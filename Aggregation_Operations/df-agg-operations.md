# PySpark DataFrame Aggregation Methods

Aggregation functions help summarize and compute statistics on grouped data. Below are some commonly used aggregation methods in PySpark.

---

## 1. Count (`count()`)

**Syntax:**
```python
df.groupBy("column_name").count()
```

**Example:**
```python
df.groupBy("role").count().show()
```

**Explanation:**  
- This groups the DataFrame by the `role` column and counts the number of records in each group. 
- For example, if there are three batters, two bowlers, and one all-rounder, the result will display counts for each category.

---

## 2. Sum (`sum()`)

**Syntax:**
```python
df.groupBy("column_name").sum("numeric_column")
```

**Example:**
```python
df.groupBy("role").sum("matches_played").show()
```

**Explanation:**  
- This groups the DataFrame by `role` and calculates the total sum of `matches_played` for each role. 
- If batters played a total of 150 matches and bowlers played 100 matches, the result will reflect those summed values.

---

## 3. Average (`avg()`)

**Syntax:**
```python
df.groupBy("column_name").avg("numeric_column")
```

**Example:**
```python
df.groupBy("role").avg("runs_scored").show()
```

**Explanation:**  
This groups the DataFrame by `role` and computes the average of **`runs_scored`** for each role. If a player have runs of 45.0, 50.5, and 40.0, the result will show the mean value (45.17).

---

## 4. Maximum (`max()`)

**Syntax:**
```python
df.groupBy("column_name").max("numeric_column")
```

**Example:**
```python
df.groupBy("role").max("runs_scored").show()
```

**Explanation:**  
- This finds the highest `runs_scored` for each role. 
- If batters have scores of 120, 150, and 98, the result will display 150 as the maximum score.

---

## 5. Minimum (`min()`)

**Syntax:**
```python
df.groupBy("column_name").min("numeric_column")
```

**Example:**
```python
df.groupBy("role").min("runs_scored").show()
```

**Explanation:**  
- This finds the lowest `runs_scored` for each role. 
- If bowlers have scores of 30, 25, and 10, the result will display 10 as the minimum value.

---

## 6. Multiple Aggregations (`agg()`)

**Syntax:**
```python
df.groupBy("column_name").agg(aggregation_function)
```

**Example:**
```python
from pyspark.sql.functions import sum, avg, max

df.groupBy("role").agg(
    sum("runs_scored").alias("total_runs"),
    avg("batting_average").alias("avg_batting_avg"),
    max("highest_score").alias("highest_score")
).show()
```

**Explanation:**  
This performs multiple aggregations at once:
- `sum("runs_scored")` calculates the total runs scored for each role.
- `avg("batting_average")` computes the average batting average for each role.
- `max("highest_score")` finds the highest individual score for each role.

- For example, if batters have total runs of 5000, an average batting average of 48.5, and a highest score of 200, these values will be shown in the output.

---

> **Note:**  
> These are just a few aggregation functions available in PySpark. You can explore more functions like `variance()`, `stddev()`, and `approx_count_distinct()`.🚀
