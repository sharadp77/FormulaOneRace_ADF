# Databricks notebook source
# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DateType
from pyspark.sql.functions import col,lit,concat,regexp_replace
from datetime import datetime
# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Define current date value "YYYY-mm-dd"
current_dt = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

def create_csv_dataframe(input_location, schema):
    """
    This function is used for creating a spark dataframe on csv file location
    :input_location: Provide input_file location of csv file
    :schema        : Provide input schema
    :return        : spark dataframe
    """
    return spark.read.csv(path=input_location, schema=schema)

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format(
            "parquet"
        ).saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name).distinct().collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list
