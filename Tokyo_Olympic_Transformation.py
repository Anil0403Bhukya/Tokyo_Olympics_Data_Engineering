# Databricks notebook source
# Azure authentication credentials are intentionally excluded from GitHub.
# Configure authentication securely in Databricks.





dbutils.fs.ls('/') # Mount operation may be restricted in current environment


# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/"))

# COMMAND ----------

df= spark.read.csv("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/athletes.csv")
df= spark.read.csv("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/teams.csv")
df= spark.read.csv("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/medals.csv")
df= spark.read.csv("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/EntriesGender.csv")
df= spark.read.csv("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/coaches.csv")


# COMMAND ----------

Athletes = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/Athletes.csv")

display(Athletes)

# COMMAND ----------

Coaches = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/coaches.csv")

display(Coaches)

# COMMAND ----------

Medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/medals.csv")
display(Medals)

# COMMAND ----------

Teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/teams.csv")

display(Teams)

# COMMAND ----------

Gender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/raw-data/EntriesGender.csv")
display(Gender)

# COMMAND ----------

Athletes_df= Athletes.distinct()

display(Athletes_df)



# COMMAND ----------

df2= Athletes.dropDuplicates(["Name", "Discipline", "NOC"])

display(df2)

# COMMAND ----------

teams_df5= Teams.distinct()
Athletes_df1= Athletes.distinct()
Coaches_df3= Coaches.distinct()
Gender_df2= Gender.distinct()
Medals_df4= Medals.distinct()


df1= Athletes.dropDuplicates()
df2= Gender.dropDuplicates()
df3 = Coaches.dropDuplicates()
df4= Medals.dropDuplicates()
df5 = Teams.dropDuplicates()

# COMMAND ----------

display(teams_df5)
display(Athletes_df1)
display(Coaches_df3)
display(Gender_df2)
display(Medals_df4)


# COMMAND ----------

display(df1)
display(df2)
display(df3)
display(df4)
display(df5)

# COMMAND ----------

from pyspark.sql.functions import col

Medals = Medals.withColumnRenamed("Rank by Total", "Rank_by_Total")

Medals.write.format("delta").mode("overwrite").save(bronze + "/medals")

# COMMAND ----------

bronze= "abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/bronze"

Athletes.write.format("delta").mode("overwrite").save(bronze+"/Athletes")

Coaches.write.format("delta").mode("overwrite").save(bronze+"/Coaches")

Gender.write.format("delta").mode("overwrite").save(bronze+"/gender")


Teams.write.format("delta").mode("overwrite").save(bronze+"/teams")

print("Bronze layer loaded successfully")


# COMMAND ----------

from pyspark.sql.functions import col

Medals = Medals.withColumnRenamed("Rank by Total", "Rank_by_Total")

Medals.write.format("delta").mode("overwrite").save(bronze + "/medals")

# COMMAND ----------

Medals = Medals.withColumnRenamed("Rank by Total", "Rank_by_Total")
df4 = Medals.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col

Medals = Medals.withColumnRenamed("Rank by Total", "Rank_by_Total")

Medals.write.format("delta").mode("overwrite").save(bronze + "/medals")

# COMMAND ----------

silver = "abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/silver"

df1.write.format("delta").mode("overwrite").save(silver + "/athletes")

df2.write.format("delta").mode("overwrite").save(silver + "/gender")

df3.write.format("delta").mode("overwrite").save(silver + "/coaches")

df4.write.format("delta").mode("overwrite").save(silver + "/medals")

df5.write.format("delta").mode("overwrite").save(silver + "/teams")

print("Silver layer loaded successfully")

# COMMAND ----------

display(dbutils.fs.ls(silver))

# COMMAND ----------

athletes = spark.read.format("delta").load(silver + "/athletes")

display(athletes)

# COMMAND ----------

coaches = spark.read.format("delta").load(silver + "/coaches")
gender = spark.read.format("delta").load(silver + "/gender")
medals = spark.read.format("delta").load(silver + "/medals")
teams = spark.read.format("delta").load(silver + "/teams")

print("Coaches:", coaches.count())
print("Gender:", gender.count())
print("Medals:", medals.count())
print("Teams:", teams.count())

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

gold_medal = medals.select(
    col("Team/NOC").alias("Country"),
    col("Gold"),
    col("Silver"),
    col("Bronze"),
    col("Total"),
    col("Rank")
)

display(gold_medal)

# COMMAND ----------

gold = "abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/gold"

gold_medal.write.format("delta").mode("overwrite").save(gold + "/country")

print("Gold medal summary loaded successfully")

# COMMAND ----------

display(dbutils.fs.ls(gold))

# COMMAND ----------

gold_gender = gender.select(
    col("Discipline"),
    col("Female"),
    col("Male"),
    col("Total")
)

display(gold_gender)

# COMMAND ----------

gold_gender.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold + "/gender")

print("Gender participation loaded successfully")

# COMMAND ----------

medals.select(
    "Rank",
    "Team/NOC",
    "Gold",
    "Silver",
    "Bronze",
    "Total",
    "Rank_by_Total"
).orderBy("Rank").show(20, False)

# COMMAND ----------

dbutils.fs.rm(gold + "/country_medal", True)
dbutils.fs.rm(gold + "/gold_medals", True)
dbutils.fs.rm(gold + "/silver_medals", True)
dbutils.fs.rm(gold + "/bronze_medals", True)

print("Unnecessary Gold datasets deleted")

# COMMAND ----------

display(dbutils.fs.ls(gold))

# COMMAND ----------

country_gold = spark.read.format("delta").load(gold + "/country")

country_gold.printSchema()
display(country_gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 2020_olympics.default.country
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/gold/country';

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in 2020_olympics.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from 2020_olympics.default.country;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE 2020_olympics.default.country;
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE 2020_olympics.default.country DROP COLUMN Number_of_Players;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE 2020_olympics.default.country;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO 2020_olympics.default.country AS c
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         NOC,
# MAGIC         COUNT(DISTINCT Name) AS Total_Players
# MAGIC     FROM delta.`abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/silver/athletes`
# MAGIC     GROUP BY NOC
# MAGIC ) AS a
# MAGIC ON c.Country = a.NOC
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET c.Total_Players = a.Total_Players;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Country,
# MAGIC     Gold,
# MAGIC     Silver,
# MAGIC     Bronze,
# MAGIC     Total,
# MAGIC     Rank,
# MAGIC     Total_Players
# MAGIC FROM 2020_olympics.default.country
# MAGIC ORDER BY Rank
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE 2020_olympics.default.sport
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     Discipline AS Sport,
# MAGIC     COUNT(DISTINCT Name) AS Number_of_Players
# MAGIC FROM delta.`abfss://tokyo-olympic-data@tokyo20.dfs.core.windows.net/silver/athletes`
# MAGIC GROUP BY Discipline
# MAGIC ORDER BY Number_of_Players DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM 2020_olympics.default.sport
# MAGIC ORDER BY Number_of_Players DESC;