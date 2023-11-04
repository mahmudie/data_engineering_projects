# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Ingestion
# MAGIC This uses a for loop to call a function to ingest football results stored in CSV format for each country

# COMMAND ----------

# MAGIC %run "../02_Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../02_Includes/configuration"

# COMMAND ----------

# Defining data types
df_type = {
    "home":str,
    "away":str,
    "date":str,
    "gh":int,
    "ga":int,
    "full_time":str,
    "competition":str,
    "home_country":str,
    "away_country":str,
    "home_code":str,
    "away_code":str,
    "home_continent":str,
    "away_continent":str,
    "continent":str,
    "continentid":str,
    "competitionid":str
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Read list of files from CSV file saved in the ADLS storage**

# COMMAND ----------

csv_files_df = spark.read.option("header",True).csv(f"{footballsource_folder_path}/file_names.csv")

# COMMAND ----------

source_list= csv_files_df.select("url").distinct().collect()
file_name_list=csv_files_df.select("file_name").distinct().collect()
source_name =""
for source in source_list:
    source_name=source.url

file_names=[]
for file_name in file_name_list:
    file_names.append(file_name.file_name)

# COMMAND ----------

# loop to extract and store csv files
for f in file_names:
    create_table(source_name,footballingested_folder_path,f,"csv","csv","fooball-data",df_type)