# Databricks notebook source
# MAGIC %md
# MAGIC #### Modeling Data
# MAGIC Split ingested data into following tables
# MAGIC 1. continents
# MAGIC 2. competitions
# MAGIC 3. apearances

# COMMAND ----------

# MAGIC
# MAGIC %run "../02_Includes/configuration"

# COMMAND ----------

# Import functions used in this notebook
from pyspark.sql.functions import sum, when, col, count, rank, desc, left,lit,substring,concat, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read football matches "delta" table

# COMMAND ----------

# Load football_matches delta data into Spark DataFrame
football_df = spark.read.format("delta").load(f"{footballprocessed_folder_path}/football_matches")

# COMMAND ----------

display(football_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create continents table

# COMMAND ----------


football_con = football_df.groupBy("continentid","continent","home_continent","away_continent")\
    .agg(count("level").alias("total_matches"))\
    .withColumn("update_date",current_timestamp())

# COMMAND ----------

display(football_con)

# COMMAND ----------

# Save continent dataset to database
football_con.write.mode("overwrite").option("mergeSchema",True).format("delta").saveAsTable("football.continents")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create competitions table

# COMMAND ----------

football_competitions = football_df.groupBy("competitionid","home_country","away_country","competition","home_code","away_code")\
    .agg(count("level").alias("total_matches"))\
    .withColumn("update_date",current_timestamp())

# COMMAND ----------

display(football_competitions)

# COMMAND ----------

# Save competitions dataset to database
football_competitions.write.mode("overwrite").option("mergeSchema",True).format("delta").saveAsTable("football.competitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create appearances table

# COMMAND ----------

football_appearances_df =football_df.select("home","away","date","gh","ga","full_time","continentid","competitionid")

# COMMAND ----------

display(football_appearances_df.count())

# COMMAND ----------

# Save appearances dataset to database
football_appearances_df.write.mode("overwrite").format("delta").saveAsTable("football.appearances")