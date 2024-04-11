# Databricks notebook source
# Databricks notebook source
# dbutils.widgets.text("Storage_Account_Name","bwtformulaonerace")
# storage_account_name = dbutils.widgets.get('Storage_Account_Name')
# dbutils.widgets.text("container_name","")
# container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get Secrets from key vault
    Application_client_ID = dbutils.secrets.get(scope="formulaonerace-scope", key="kv-FormulaoneRace-Application-client-ID")
    Directory_Tenant_Id = dbutils.secrets.get(scope="formulaonerace-scope",key="kv-FormulaoneRace-Directory-Tenant-Id")
    client_secret_value = dbutils.secrets.get(scope="formulaonerace-scope",key="kv-FormulaoneRace-client-secret-value")

    # Set spark Configs
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": Application_client_ID,
               "fs.azure.account.oauth2.client.secret": client_secret_value,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{Directory_Tenant_Id}/oauth2/token"}
    
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount Storage Account Containers
    dbutils.fs.mount(
             source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
             mount_point = f"/mnt/{storage_account_name}/{container_name}",
             extra_configs = configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

# mount_adls(f"{storage_account_name}",f"{container_name}")
mount_adls("bwtformulaonerace","silver")
mount_adls("bwtformulaonerace","gold")
mount_adls("bwtformulaonerace","bronze")

# COMMAND ----------

dbutils.fs.ls("/mnt/bwtformulaonerace/bronze/")
