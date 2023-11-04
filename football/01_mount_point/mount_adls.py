# Databricks notebook source
# MAGIC %md
# MAGIC #### ADLS Mounter funtion
# MAGIC 1. This function is used to amount a specific container from ADLS to the Azure Databricks
# MAGIC 2. The function takes two parameters storage account name as first parameter and container name as the second parameter
# MAGIC 3. It uses Service Princial to access ADLS

# COMMAND ----------

# function defination
def mount_point(storage_account_name,container_name):
    # 
    client_id=dbutils.secrets.get(scope='formula1-scope',key='Formula-app-client-id')
    tenant_id=dbutils.secrets.get(scope='formula1-scope',key='Formula1-tenant-id')
    client_secret=dbutils.secrets.get(scope='formula1-scope',key='Formula1-secret-value')

    configs ={"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
     # mount 
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)   
    # Display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databricksdl2/footballingested"))

# COMMAND ----------

mount_point("databricksdl2","footballflags")


# COMMAND ----------

mount_point("databricksdl2","footballingested")

# COMMAND ----------

mount_point("databricksdl2","footballprocessed")

# COMMAND ----------

mount_point("databricksdl2","footballsource")