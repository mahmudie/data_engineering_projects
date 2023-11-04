# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run "../02_Includes/configuration"

# COMMAND ----------

# **function to fill time stamp the data set were ingested, data source, and table name columns**
from pyspark.sql.functions import current_timestamp, lit
def create_new_column(input_df, file_name, source_name):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())\
        .withColumn("data_source_name",lit(source_name))\
        .withColumn("file_name",lit(file_name))
    return output_df

# COMMAND ----------

# **This function helps in converting string to date format**
from datetime import datetime
def convert_to_date(input_string):
    year = int(input_string[0:4])
    month =int(input_string[5:7])
    day=int(input_string[8:])
    date_c=datetime(year, month, day)
    return date_c

# COMMAND ----------


# **Function to ingest data from the source given as parameter and dynamically save the Spark Data Frame into CSV for further proccessig**
def create_table(source_path,destination_path,table_name,file_format_source,file_format_destination,data_source_name,df_type):
    import pandas as pd
    data_source_url = f"{source_path}/{table_name}.{file_format_source}" 
    # Read data from the source using Pandas
    extracted_python_df = pd.read_csv(data_source_url)
    extracted_python_df['continentid']=extracted_python_df['home_continent'].str[:2].apply(lambda x:x.upper())+extracted_python_df['away_continent'].str[:2].apply(lambda x:x.upper())+extracted_python_df['continent'].str[:2].apply(lambda x:x.upper())

    extracted_python_df['competitionid']=extracted_python_df['away_country'].str[:4].apply(lambda x:x.upper())+extracted_python_df['home_country'].str[:4].apply(lambda x:x.upper())+extracted_python_df['competition'].str[:4].apply(lambda x:x.upper())+'-'+extracted_python_df['home_code']+"-" +extracted_python_df['away_code']

    # Read data from Pandas data from and convert schema
    extracted_spark_df = spark.createDataFrame(extracted_python_df.astype(df_type))
    # Add new columns to the Spark Data Frame
    extracted_spark_renamed_df=create_new_column(extracted_spark_df,table_name, data_source_name)

    # Save spark data frame into ADLS storage account
    extracted_spark_renamed_df.write.format(file_format_destination).mode("overwrite").save(f"{footballingested_folder_path}/{table_name}")
    # display(extracted_spark_df)

# COMMAND ----------

def movepartition (df,partitionId):
    df_columns =df.schema.names
    final_df=df
    if partitionId !="0" and partitionId in df_columns:
        df_columns.remove(partitionId)
        df_columns.append(partitionId)
        final_df =df.select(df_columns)
    else:
        final_df =df
        
    return final_df

# COMMAND ----------

def merge_delta_data(df,database_name,table_name, folder_path,merge_condition,partitionId):
    from delta.tables import DeltaTable
    if(spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark,f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge (
            df.alias("src"),merge_condition
        )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        df.write.mode("overwrite").partitionBy(partitionId).format("delta").saveAsTable(f"{database_name}.{table_name}")