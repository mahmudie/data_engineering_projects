-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop tables and database and then re-create databases

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP DATABASE IF EXISTS football;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS football
LOCATION "/mnt/databricksdl2/footballprocessed"