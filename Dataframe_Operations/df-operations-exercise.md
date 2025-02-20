# PySpark DataFrame Operations Exercise
---

## Exercise 1: Create a Dataframe and do the following operation

1. Create a DataFrame with four columns: `Name`, `Age`, `City` and `Country`.
2. Insert the following five records:
   - ("Mark Boucher", 42, "Cape Town", "South Africa")
   - ("Shaun Pollock", 48, "Durban", "South Africa")
   - ("Brendon McCullam", 39, "Wellington", "New ZeaLand")
   - ("Saurav Ganguly", 44, "Mumbai", "India")
   - ("Shoaib Akhtar", 41, "Rawalpindi", "Pakistan")
3. Display the DataFrame.
4. Show the dataframe with only the following columns: `Name` and `Country`.
5. Show the dataframe sorted by the column `Age` in *descending* order.
6. Show the columns available in the dataframe.
7. Drop the column `Age` from the Dataframe.
8. Show the schema of the dataframe as of now.
9. Show the records from the dataframe where the `Country` is **South Africa**.

---

## Exercise 2: Read a csv file and Create a Dataframe using it, with specific data types and do the following operation (file name: players1.csv)

1. Read the csv file and create its dataframe with following schema
   - `first_name` (StringType)
   - `last_name` (StringType)
   - `gender` (StringType)
   - `age` (IntegerType)
2. Display the DataFrame.
3. Show only the first five records of the DataFrame.
4. Show the number of counts of each `gender`.
5. Show the `first_name`, `last_name` and the `age` of the player who is the youngest in the whole dataframe.
6. Show the `first_name`, `last_name`, `gender` and the `age` of the player who is the oldest among their gender in the whole dataframe.
7. Add another column `full_name` which would be the concatenation of `first_name` and `last_name` in the dataframe.
8. Show the schema of the dataframe.
9. Show the records for those who have `age` greater than 35.
 
---


| References |
| ---------- |
**[Creating Dataframes](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframes/dataframes.md#creating-dataframes-in-pyspark)**
**[Dataframe Operations](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframe_Operations/df-operations.md#pyspark-dataframe-operations)**
**[Solutions](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframe_Operations/df-operations-solutions.md)**
**[Download CSV](https://github.com/RahulRoy-rsp/Learning_PySpark/tree/main/Dataframe_Operations/csv-files)**
**[Notebook Solution](https://github.com/RahulRoy-rsp/Learning_PySpark/blob/main/Dataframe_Operations/df_operations_exercise.ipynb)**

---
**[Back to Home Page](https://github.com/RahulRoy-rsp/Learning_PySpark)**
