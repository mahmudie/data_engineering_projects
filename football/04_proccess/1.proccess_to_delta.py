# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Proccessing
# MAGIC This notebook reads ingested data and convert them into **delta** format by creating two tables **countryflags** and **football_matches**.

# COMMAND ----------

# MAGIC %run "../02_Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../02_Includes/configuration"

# COMMAND ----------

football_files_df=spark.read.option("header",True).format("csv").load(f"{footballsource_folder_path}/file_names.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating delta table **Countryflags**

# COMMAND ----------

# Read ingested data
country_flags=spark.read.option("header",True).format("csv").load(f"{footballflags_folder_path}/countryflags")

# COMMAND ----------

display(country_flags)

# COMMAND ----------

# Renaming columns for country flags
countries_df=spark.read.format("csv").load(f"{footballflags_folder_path}/countryflags")\
    .withColumnRenamed("_c0","country")\
    .withColumnRenamed("_c1","flag")\
    .withColumnRenamed("_c2","ingestion_date")\
    .withColumnRenamed("_c3","data_source")

# COMMAND ----------

# Saving countryflags table into "delta" table format
countries_df.write.mode("overwrite").format("delta").saveAsTable("football.countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating delta table **football_matches**

# COMMAND ----------

file_name_list= football_files_df.select("file_name").distinct().collect()
file_names=[]
for file_name in file_name_list:
    file_names.append(file_name.file_name.lower())

country_list_collection=countries_df.select("country").distinct().collect()
country_List =[]
for country in country_list_collection:
    country_List.append(country.country.lower())

# COMMAND ----------

rename_columns={"_c0":"home","_c1":"away","_c2":"date","_c3":"gh","_c4":"ga","_c5":"full_time","_c6":"competition","_c7":"home_ident",
                "_c8":"away_ident","_c9":"home_country","_c10":"away_country","_c11":"home_code","_c12":"away_code",
                "_c13":"home_continent","_c14":"away_continent","_c15":"continent","_c16":"level","_c17":"continentid",
                "_c18":"competitionid","_c19":"ingestion_date","_c20":"data_source","_c21":"file_name"}



# COMMAND ----------

count=0
total_records=0
football_matches_df = spark.read.csv(f"{footballingested_folder_path}/aruba")

total_records=football_matches_df.count()
for country in file_names:
    count+=1
    country2=country.replace("-"," ")
    # country2=country2.title()
    if country2 in country_List:
        football_country_df=spark.read.csv(f"{footballingested_folder_path}/{country}")
        total_records+=football_country_df.count()
        football_matches_df=football_matches_df.union(football_country_df)

# COMMAND ----------

football_matches_df=football_matches_df.withColumnsRenamed(rename_columns)

# COMMAND ----------

display(football_matches_df.count())

# COMMAND ----------

display(football_matches_df)

# COMMAND ----------

football_matches_df.write.mode("overwrite").option("mergeSchema",True).partitionBy("continent").format("delta").saveAsTable("football.football_matches")