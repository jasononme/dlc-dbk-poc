# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `banking_da_db`.`0014000000nxts8`.`upentity_output_v1`;

# COMMAND ----------

import json

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, schema_of_json, expr

# COMMAND ----------

def get_json_schema_from_parquet(s3_path: str, column_name: str) -> str:
    """ Grabs an s3_path of a parquet and column_name then returns a string to define a schema for a json"""
    return spark.read.parquet(s3_path).select(column_name).head()[0]

upentity_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/upentity",
    column_name="jsondoc_"
    )

historicalstatement_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/historicalstatement",
    column_name="jsondoc_"
    )

ratingscenarioblockdata_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/ratingscenarioblockdata",
    column_name="jsondoc_"
    )
    
b = F.schema_of_json(upentity_schema)

display(upentity_schema)
display(b)

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import dlt
# MAGIC
# MAGIC
# MAGIC df = spark.readStream.format("delta").table("banking_da_db.0014000000nxts8.upentity_output_v1").alias('o')
# MAGIC col_iterim = F.from_json("o.jsondoc_", F.schema_of_json(upentity_schema)).getItem('Data').alias('_')
# MAGIC display(df.select('*', col_iterim).select('o.*', '_.*'))
# MAGIC # display(df.select(expr("from_json('jsondoc_:Data', schema_of_json('jsondoc_:Data'))")))
# MAGIC # display(df.select(expr("schema_of_json(jsondoc_:Data)")))
# MAGIC
# MAGIC # dlt.read_stream("banking_da_db.0014000000nxts8.upentity_output_v1")
# MAGIC   # .select("*", F.from_json("jsondoc_", F.schema_of_json(upentity_schema)).alias("jsondoc_2"))
# MAGIC   # .select("*", "jsondoc_2.Data", "jsondoc_2.FinancialId", "jsondoc_2.UserId", "jsondoc_2.moduleId_"))
# MAGIC   # .select("*", "Data.*", F.current_timestamp().alias("time"))
# MAGIC

# COMMAND ----------

df = spark.readStream.format("delta").table("banking_da_db.0014000000nxts8.historicalstatement_output_v1").alias('o')
display(df)
# col_iterim = F.from_json("o.jsondoc_", F.schema_of_json(historicalstatement_schema)).getItem('Data').alias('_')
# display(df.select('*', col_iterim).select('o.*', '_.*'))

# COMMAND ----------

df = spark.readStream.format("delta").table("banking_da_db.0014000000nxts8.ratingscenarioblockdata_output_v1").alias('o')
# display(df)
col_iterim = F.from_json("o.jsondoc_", F.schema_of_json(ratingscenarioblockdata_schema))
# col_iterim = F.from_json("o.jsondoc_", F.schema_of_json(ratingscenarioblockdata_schema)).getItem('RatingBlockPinDatum').alias('_')
# display(df.select('*', col_iterim).select('o.*', '_.*'))

display(df.select('*', col_iterim))

# COMMAND ----------

def expand_creditlens_dataframe_json_field(table): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    # df = spark.readStream.format("delta").table(<table_full_name>).alias('o')
    # df = dlt.read_stream(sample_table_name_merged).alias('o')
    df = spark.readStream.format("delta").table(f'banking_da_db.0014000000nxts8.{sample_table_name_merged}').alias('o')
    # Get json schema for table
    table_schema = get_json_schema_for_source_table(table)
    col_iterim_base = F.from_json("o.jsondoc_", F.schema_of_json(table_schema))
    col_alias = []
    iterim_cols = []
    index = 0
    if table in table_nested_json:
        # Might more than one field - using `.` as delimiter
        nested_json_fields = table_nested_json[table]
        nested_json_fields = OrderedDict(zip(nested_json_fields, nested_json_fields))
        for field in [i for i in nested_json_fields if i]: 
            iterim_col = col_iterim_base
            field_comps = field.split('.')
            for item in field_comps: 
                iterim_col = iterim_col.getItem(item)
            iterim_cols.append(iterim_col)
            iterim_col_alias = f'_{index}'
            iterim_col.alias(iterim_col_alias)
            col_alias.append(iterim_col_alias)
            index += 1
    else: 
        iterim_col_alias = f'_{index}'
        col_iterim_base.alias(iterim_col_alias)
        iterim_cols.append(col_iterim_base)
        col_alias.append(iterim_col_alias)
    
    # return df.select('o.*', *iterim_cols).select('o.*', *[f'{i}.*' for i in col_alias])
    return df.select('*', *iterim_cols)

# COMMAND ----------

a = expand_creditlens_dataframe_json_field('entity')
display(a)
