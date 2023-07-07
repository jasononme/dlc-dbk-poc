# Databricks notebook source
import json

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import expr, col


# COMMAND ----------

version = '_v1'
data_file_location_root = 's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/'
schema_location_root = 's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/meta_info/_temp/poc1/20230629/'
tables = ['entity', 'activetesting', 'covenant', 'financial', 'upentity', 'historicalstatement']

# COMMAND ----------

# @dlt.create_table(name="entity_input_cdc_v1")
# tables = ['entity', 'activetesting', 'covenant', 'financial', 'upentity', 'historicalstatement']
default_tables = ['entity']

# configuration
tenant_id = spark.conf.get("mypipeline.tenant_id", None) or ""
tenant_id_prefix = f'{tenant_id}_' if tenant_id else ''
table_conf = spark.conf.get("mypipeline.tables", None) or ""
tables = [t_strip for t in table_conf.split(',') if (t_strip := t.strip())]

if not tables: 
    tables = list(default_tables)

def path_exists(path): 
    try: 
        dbutils.fs.ls(path)
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise
    else: 
        return True

def generate_cnx_dlt(table): 

    # table_name = f'{tenant_id_prefix}{table}_input_cdc{version}'
    table_name = f'{table}_input_cdc{version}'

    # add tenant portion later
    table_file_location = f'{data_file_location_root.rstrip("/")}/{table}/'
    exists = path_exists(table_file_location)
    print(f'{table_file_location}: {exists}')
    
    # TODO Potentially we could add expectation of required versionid_(by CL), and commit_time (by DMS)
    @dlt.create_table(name=table_name)
    def cnx_dlt():
        schema_file_location = f'{schema_location_root.rstrip("/")}/_{tenant_id_prefix}{table}/_dlt{version}'
        
        df = (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.partitionColumns", "")
            .option("cloudFiles.schemaLocation", schema_file_location)
            .load(table_file_location)
            
        )

        # Initial full-load, won't have `Op` column
        df = df.select(
            col('Op') if 'Op' in df.columns else expr("'' as Op"), 
            *[c for c in df.columns if c not in {'Op'}], 
            expr('current_timestamp() as _processing_time'), 
            expr(f"'{tenant_id}' as tenant_id"), 
        )

        # Initial full-load, won't have `Op` column
        # if (not df.columns.contains("Op")): 
        #     df = df.withColumn("Op", lit(""))

        return df

# [generate_cnx_dlt(table) for table in tables]

for table in tables: 
    generate_cnx_dlt(table)


# COMMAND ----------


def generate_cnx_dlt_merged(table): 
    sample_table_name_source = f'{table}_input_cdc{version}'
    sample_table_name_merged = f'{table}_input_merged{version}'
    dlt.create_streaming_table(sample_table_name_merged)

    dlt.apply_changes(
        target = sample_table_name_merged,
        source = sample_table_name_source,
        keys = ["id_"],
        #sequence_by = F.col("case when "),
        #sequence_by = expr("(case when updateddate_ is not null then updateddate_ else createddate_ end) as sequence_"),
        sequence_by = expr("(''||versionid_||'_'||commit_time) as sequence_"),
        #sequence_by = F.col("commit_time"),
        apply_as_deletes = expr("Op = 'D'"),
        # apply_as_truncates = expr("operation = 'TRUNCATE'"),
        except_column_list = ["Op"],
        stored_as_scd_type = 1
    )

# For debugging
#tables_to_merge = ['entity']

tables_to_merge = tables
for table in tables_to_merge: 
    generate_cnx_dlt_merged(table)

# COMMAND ----------

# This might be used for certain pre-handling
def generate_cnx_dlt_output(table): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    sample_table_name_output = f'{table}_output{version}'
    @dlt.table(name=sample_table_name_output)
    @dlt.expect("valid_op_date", "updateddate_ IS NOT NULL or createddate_ is not null")
    def cnx_dlt_output():
        return dlt.read_stream(sample_table_name_merged).select('*')

# For debugging
#tables_to_output = ['entity']
tables_to_output = tables
for table in tables_to_output: 
    generate_cnx_dlt_output(table)

# COMMAND ----------

# table_name ='entity_input_cdc_v1_1'
# checkpoint_path = 's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/temp_testing/meta_info/_temp/poc1/_entity/_dlt_v1_1'
# (spark.readStream
#   .format("cloudFiles")
#   .option("cloudFiles.format", "parquet")
#   .option("cloudFiles.inferColumnTypes", "true")
#   .option("cloudFiles.schemaLocation", checkpoint_path)
#   .load('s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/temp_testing/entity/')
#   .select("*", current_timestamp().alias("processing_time"))
#   .writeStream
#   .option("checkpointLocation", checkpoint_path)
#   .trigger(availableNow=True)
#   .toTable(table_name))
