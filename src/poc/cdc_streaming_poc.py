# Databricks notebook source
import json

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import expr, col
from collections import OrderedDict


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

table_nested_json = {
    'entityindustry': ['IndustryCode'], 
    'upentity': ['Data'],
}

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
    
@dlt.table(name="lookup_customrefdata_input")
def lookup_customrefdata():
    schema_file_location = f'{schema_location_root.rstrip("/")}/_{tenant_id_prefix}lookup_customrefdata_input/_dlt{version}'
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_file_location)
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/jsondatasamples/lookup/lookup_customrefdata")
        .select("*", F.current_timestamp().alias("time"))
    )

def resolve_table_file_location(table: str): 
    return f'{data_file_location_root.rstrip("/")}/{table}/'

def get_json_schema_from_parquet(s3_path: str, column_name: str) -> str:
    """ Grabs an s3_path of a parquet and column_name then returns a string to define a schema for a json"""
    return spark.read.parquet(s3_path).select(column_name).head()[0]

def get_json_schema_for_source_table(table_name: str) -> str:
    s3_path = resolve_table_file_location(table_name)
    return get_json_schema_from_parquet(s3_path, 'jsondoc_')

def generate_cnx_dlt(table): 

    # table_name = f'{tenant_id_prefix}{table}_input_cdc{version}'
    table_name = f'{table}_input_cdc{version}'

    # add tenant portion later
    table_file_location = resolve_table_file_location(table)
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
            # expr("iff(Op = 'D', 'DELETE'") if 'Op' in df.columns else expr("'' as Op"), 
            # For full-load, there is no `transact_seq`, thus, add fake one based on commit time (which is the full-load time for this scenario)
            # expr(
            #     "iff(transact_seq is not null and transact_seq != '', transact_seq, date_format(commit_time, 'yyyyMMddhhmmssSSS000000000000000000')) as transact_seq"),
            col('commit_time'),
            col('transact_seq'),
            *[c for c in df.columns if c not in {'Op', 'transact_seq', 'commit_time'}], 
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
        keys = ["pkid_", "versionid_"],
        #sequence_by = F.col("case when "),
        #sequence_by = expr("(case when updateddate_ is not null then updateddate_ else createddate_ end) as sequence_"),
        # sequence_by = expr("(''||versionid_||'_'||commit_time) as sequence_"),
        sequence_by = F.col("commit_time"),
        # sequence_by = F.col("transact_seq"),
        # sequence_by = expr("(id_||'_'||transact_seq) as sequence_"),
        apply_as_deletes = expr("Op = 'D'"),
        # apply_as_truncates = expr("operation = 'TRUNCATE'"),
        except_column_list = ["Op"],
        # stored_as_scd_type = 1,
        stored_as_scd_type = 2,
        # track_history_column_list=['id_'],
        track_history_except_column_list=['jsondoc_', 'Op'],
    )

# For debugging
#tables_to_merge = ['entity']

tables_to_merge = tables
for table in tables_to_merge: 
    generate_cnx_dlt_merged(table)

# COMMAND ----------

def expand_creditlens_dataframe_json_field(table): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    # df = spark.readStream.format("delta").table(<table_full_name>).alias('o')
    # df = dlt.read_stream(sample_table_name_merged).alias('o')
    df = spark.readStream.format("delta").table(f'LIVE.{sample_table_name_merged}').alias('o')
    # Get json schema for table
    table_schema = get_json_schema_for_source_table(table)
    
    col_alias = []
    iterim_cols = []
    index = 0
    if table in table_nested_json:
        # Might more than one field - using `.` as delimiter
        nested_json_fields = table_nested_json[table]
        nested_json_fields = OrderedDict(zip(nested_json_fields, nested_json_fields))
        for field in [i for i in nested_json_fields if i]: 
            
            iterim_col_alias = f'_{index}'
            col_iterim_base = F.from_json("o.jsondoc_", F.schema_of_json(table_schema))
            iterim_col = col_iterim_base
            field_comps = field.split('.')
            for item in field_comps: 
                iterim_col = iterim_col.getItem(item)
            
            iterim_col = iterim_col.alias(iterim_col_alias)
            iterim_cols.append(iterim_col)
            col_alias.append(iterim_col_alias)
            index += 1
    else: 
        
        iterim_col_alias = f'_{index}'
        col_iterim_base = F.from_json("o.jsondoc_", F.schema_of_json(table_schema))
        iterim_col = col_iterim_base.alias(iterim_col_alias)
        iterim_cols.append(iterim_col)
        col_alias.append(iterim_col_alias)
    
    return df.select('o.*', *iterim_cols).select('o.*', *[f'{i}.*' for i in col_alias])
    # return df.select('*', *iterim_cols)

# COMMAND ----------

# This might be used for certain pre-handling
def generate_cnx_dlt_output(table): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    sample_table_name_output = f'{table}_output{version}'
    @dlt.table(name=sample_table_name_output)
    @dlt.expect("valid_op_date", "updateddate_ IS NOT NULL or createddate_ is not null")
    def cnx_dlt_output():
        # return dlt.read_stream(sample_table_name_merged).select('*')
        return expand_creditlens_dataframe_json_field(table)


# For debugging
#tables_to_output = ['entity']
tables_to_output = tables
for table in tables_to_output: 
    generate_cnx_dlt_output(table)

# COMMAND ----------

# Testing codes for both scenarios: With/Without nested JSON
# @dlt.table(name='test_expanded_ei')
# @dlt.expect("valid_op_date", "updateddate_ IS NOT NULL or createddate_ is not null")
# def json_expanded_dlt():
#     return expand_creditlens_dataframe_json_field('entityindustry')

# @dlt.table(name='test_expanded_e')
# @dlt.expect("valid_op_date", "updateddate_ IS NOT NULL or createddate_ is not null")
# def json_expanded_dlt_e():
#     return expand_creditlens_dataframe_json_field('entity')


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
