# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Ingestion
# MAGIC Using **BeautifulSoup** to extract country names and flags url and store in ADLS

# COMMAND ----------

# MAGIC %run "../02_Includes/common_functions"

# COMMAND ----------

# MAGIC     %run "../02_Includes/configuration"

# COMMAND ----------

import requests
response = requests.get("https://www.worldometers.info/geography/flags-of-the-world/")
if response.status_code != 200:
	print("Error fetching page")
	exit()
else:
	content = response.content

# COMMAND ----------

from bs4 import BeautifulSoup
import pandas as pd
soup = BeautifulSoup(response.content, 'html.parser')

# COMMAND ----------

site_address='https://www.worldometers.info'

# COMMAND ----------

second_child_div = soup.findAll('div', {'class': 'col-md-4'})

# COMMAND ----------

df = pd.DataFrame(columns = ['country', 'flag'])
count=0;
for i in second_child_div:
    link =site_address+i.find("a")['href']
    country=i.text
    count+=1
    df=df.append({"country":country,"flag":link},ignore_index = True)

# COMMAND ----------

# Defining data types
df_type = {
    "country":str,
    "flag":str
}

# COMMAND ----------

 extracted_spark_df = spark.createDataFrame(df.astype(df_type))
# Add new columns to the Spark Data Frame
extracted_spark_column_df=create_new_column(extracted_spark_df,"countryflags", "https://www.worldometers.info")
extracted_spark_renamed_df=extracted_spark_column_df\
    .withColumnRenamed("_c0","country")\
    .withColumnRenamed("_c1","flag")\
    .withColumnRenamed("_c2","ingestion_date")\
    .withColumnRenamed("_c3","data_source_name")
 extracted_spark_renamed_df=  extracted_spark_renamed_df.select("country","flag","ingestion_date","data_source_name")
# Save spark data frame into ADLS storage account
extracted_spark_renamed_df.write.format("csv").mode("overwrite").save(f"{footballflags_folder_path}/countryflags")

# COMMAND ----------

display(spark.read.format("csv").load(f"{footballflags_folder_path}/countryflags")
    .withColumnRenamed("_c0","country")\
    .withColumnRenamed("_c1","flag")\
    .withColumnRenamed("_c2","ingestion_date")\
    .withColumnRenamed("_c3","data_source"))