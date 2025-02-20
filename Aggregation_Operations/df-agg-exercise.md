# PySpark DataFrame Aggregation Operations Exercise
---

## Exercise 1: Create a Dataframe with specific data types and do the following operation

1. Create a DataFrame with columns:
   - `player_name` (StringType)
   - `matches` (IntegerType)
   - `goals_scored` (IntegerType)
   - `team_name` (StringType)

2. Insert the following records:
   - ("Christiano Ronaldo", 7, 12, "Spain")
   - ("Lionel Messi", 6, 11, "Argentica")
   - ("Luka Modric", 5, 9, "Croatia")
   - ("Harry Kane", 5, 12, "England")
   - ("Vinicius Junior", 4, 7, "Brazil")
   - ("Sergio Ramos", 7, 5, "Spain")
   - ("Neymar Da Silva", 4, 9, "Brazil")

3. Display the DataFrame.
4. Show the **number of records** in the dataframe.
5. Show the dataframe with only the following columns: `player_name` and `goals_scored`.
6. Show the dataframe **sorted** by the column `goals_scored` in *descending* order. 
7. Show the **total** number of `goals_scored` in the dataframe.
8. Show the **average** number of `goals_scored` in the dataframe.
9. Show the records from the dataframe where the `team_name` is **Spain**.
10. Show the result set from the dataframe with the columns as `Max Goals`, `Min Goals`, `Total Goals`, `Average Goals` for each `team_name`.
11. Show the `player_name` who scored the most goals, (Columns to show:`player_name`, `goals_scored`).
12. Show the `player_name` who played the most matches, (Columns to show:`player_name`, `matches`).

---

## Exercise 2: Read a csv file and Create a Dataframe using it, with specific data types and do the following operation (file name: players2.csv)

1. Read the csv file and create its dataframe with following schema
   - `player_name` (StringType)
   - `age` (IntegerType)
   - `gender` (StringType)
   - `country` (StringType)
2. Display the DataFrame.
3. Show only the first five records of the DataFrame **sorted** by `age`.
4. Derive two new fields `first_name` and `last_name` using `player_name`. (first_name and last_name is separated by space).
5. Display the DataFrame.
6. Show the schema of the dataframe.
7. Derive a new field `Is_Eligible`. (Assign it as **True** if the age is less than 21, else **False**)
8. Show the records who has `Is_Eligible` as **True**.
9. Show the records who has `Is_Eligible` as **False**.

---

| References |
| ---------- |
**[Creating Dataframes](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/dataframes.md#creating-dataframes-in-pyspark)**
**[Dataframe Operations](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframe_Operations/df-operations.md#pyspark-dataframe-operations)**
**[Aggregation Operations](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Aggregation_Operations/df-agg-operations.md#pyspark-dataframe-aggregation-methods)**
**[Solutions](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Aggregation_Operations/df-operations-solutions.md)**
**[Download CSV](https://github.com/RahulRoy-rsp/Learning_PySpark/tree/main/Aggregation_Operations/csv-files)**

---
**[Back to Home Page](https://github.com/RahulRoy-rsp/Learning_PySpark)**
