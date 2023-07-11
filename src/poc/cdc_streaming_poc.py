# Databricks notebook source
import json

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import expr, col
from collections import OrderedDict


# COMMAND ----------



# COMMAND ----------

version = '_v1'
location_subfolder = 'branch1/'
data_file_location_root = 's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/'
schema_location_root = f's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/meta_info/_temp/poc1/20230629/{location_subfolder}_schema/'
table_data_location_root = f's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/delta_data/poc1/20230629/{location_subfolder}data/'
checkpoint_location_root = f's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/meta_info/_temp/poc1/20230629/{location_subfolder}_checkpoint/'
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
    'entityindustry': ['self', 'IndustryCode'], 
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
    
@dlt.table(name="lookup_customrefdata_input_landing")
def lookup_customrefdata_streaming():
    schema_file_location = f'{schema_location_root.rstrip("/")}/_{tenant_id_prefix}lookup_customrefdata_input/_dlt{version}'
    delta_table_file_location = f'{table_data_location_root.rstrip("/")}/_{tenant_id_prefix}lookup_customrefdata_input/_dlt{version}'
    checkpoint_file_location = f'{checkpoint_location_root.rstrip("/")}/_{tenant_id_prefix}lookup_customrefdata_input/_dlt{version}'

    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_file_location)
        # .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/jsondatasamples/lookup/lookup_customrefdata")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/extra_data/lookup/v1/lookup_customrefdata")
        .select("*", F.current_timestamp().alias("time"))
        # .writeStream
        # .format('delta')
        # .outputMode("append")
        # .option("checkpointLocation", checkpoint_file_location)
        # # .toTable('LIVE.lookup_customrefdata_input_landing')
        # .start(delta_table_file_location)
        # .awaitTermination()
    )


@dlt.table(name="lookup_customrefdata_input", spark_conf={"pipelines.trigger.interval" : "30 seconds"})
def lookup_customrefdata():
    return dlt.read('lookup_customrefdata_input_landing')

def resolve_table_file_location(table: str, history=False): 
    return f'{data_file_location_root.rstrip("/")}/{table}{"hist" if history else ""}/'

def get_json_schema_from_parquet(s3_path: str, column_name: str) -> str:
    """ Grabs an s3_path of a parquet and column_name then returns a string to define a schema for a json"""
    # TODO HOW TO make sure it resolve the very latest one or everything? 
    print(f's3_path: {s3_path}')
    # TODO: Should keep old if there is? Not remove column
    # TODO Need to see how to expand on-fly, instead restart
    json_value = spark.read.parquet(s3_path).filter(f'{column_name} is not null').sort(col('commit_time').desc()).select(column_name).head()[0]
    # json_value = spark.read.parquet(s3_path).select(column_name).head()[0]
    print(f'json_value[{s3_path}]: {json_value}')

    return json_value

def get_json_schema_for_source_table(table_name: str) -> str:
    s3_path = resolve_table_file_location(table_name)
    return get_json_schema_from_parquet(s3_path, 'jsondoc_')

def generate_cnx_dlt(table): 

    # table_name = f'{tenant_id_prefix}{table}_input_cdc{version}'
    table_name = f'{table}_input_cdc{version}'

    # add tenant portion later
    # table_hist = resolve_table_file_location(table, history=True)
    table_file_location = [resolve_table_file_location(table)]
    table_file_location = [i for i in table_file_location if path_exists(i)]
    exists = len(table_file_location) > 0
    print(f'{table_file_location}: {exists}')
    
    # TODO Potentially we could add expectation of required versionid_(by CL), and commit_time (by DMS)
    @dlt.create_table(name=table_name)
    def cnx_dlt():
        df_streams = []
        part_index = 0
        for table_loc in table_file_location:
            schema_file_location = f'{schema_location_root.rstrip("/")}/_{tenant_id_prefix}{table}/_dlt{version}/part{part_index}/'
            delta_table_file_location = f'{table_data_location_root.rstrip("/")}/_{tenant_id_prefix}{table}/_dlt{version}/parts/'
            checkpoint_file_location = f'{checkpoint_location_root.rstrip("/")}/_{tenant_id_prefix}{table}/_dlt{version}/part{part_index}/'
            df_stream = (spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.partitionColumns", "")
                .option("cloudFiles.schemaLocation", schema_file_location)
                .load(table_loc)
                # .writeStream
                # .outputMode("append")
                # .format('delta')
                # .option("checkpointLocation", checkpoint_file_location)
                # # .toTable(f'LIVE.{table_name}')
                # .start(delta_table_file_location)
                # .awaitTermination()
            )
            df_streams.append(df_stream)
            part_index += 1
        
        # Union from multple folders: e.g. entity/entityhist
        df = df_streams[0]
        for df_s in df_streams[1:]: 
            df = df.unionByName(df_s, allowMissingColumns=True)

        # Initial full-load, won't have `Op` column
        df = df.select(
            col('Op') if 'Op' in df.columns else expr("'' as Op"), 
            # expr("iff(Op = 'D', 'DELETE'") if 'Op' in df.columns else expr("'' as Op"), 
            # For full-load, there is no `transact_seq`, thus, add fake one based on commit time (which is the full-load time for this scenario)
            # expr(
            #     "iff(transact_seq is not null and transact_seq != '', transact_seq, date_format(commit_time, 'yyyyMMddhhmmssSSS000000000000000000')) as transact_seq"),
            col('commit_time'),
            col('transact_seq'),
            *[c for c in df.columns if c not in {'Op', 'transact_seq', 'commit_time', 'isvalid_'}], 
            expr('current_timestamp() as _processing_time'), 
            expr(f"'{tenant_id}' as tenant_id"),             
            expr("'true' as isvalid_") if 'isvalid_' not in df.columns else col('isvalid_'),  
            expr('true as _version_enabled') if 'versionid_' in df.columns else expr('true as _version_enabled'),  # this is used for distinguing versioned table or not, to detmine how ot handle `delete` op
        ).filter(
            "_version_enabled and (Op is null or Op != 'D') and isvalid_ == 'true'" # not to handle draft now
        ) # Ignore the delete event for version enabled table - since this is moving data to `child` table, e.g. `*hist`

        # df.writeStream.outputMode("append").format("memory").queryName(table_name).trigger(availableNow=True).start()

        # df.map(lambda r: {})

        # Initial full-load, won't have `Op` column
        # if (not df.columns.contains("Op")): 
        #     df = df.withColumn("Op", lit(""))

        return df.select('*')

# [generate_cnx_dlt(table) for table in tables]

for table in tables: 
    generate_cnx_dlt(table)


# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):
    print(f'>>>microBatchOutputDF: {len(microBatchOutputDF)}')


# COMMAND ----------


def generate_cnx_dlt_merged(table): 
    sample_table_name_source = f'{table}_input_cdc{version}'
    sample_table_name_merged = f'{table}_input_merged{version}'
    dlt.create_streaming_table(sample_table_name_merged)

    dlt.apply_changes(
        target = sample_table_name_merged,
        source = sample_table_name_source,
        # keys = ["pkid_", "versionid_"],
        keys = ["id_"],
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
        # track_history_except_column_list=['jsondoc_', 'Op'],
        track_history_except_column_list=['Op'],
    )

tables_to_merge = tables
# For debugging
# tables_to_merge = ['entity']
# tables_to_merge = []

for table in tables_to_merge: 
    generate_cnx_dlt_merged(table)

# COMMAND ----------


def expand_creditlens_dataframe_json_field(table, streaming=True): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    # df = spark.readStream.format("delta").table(<table_full_name>).alias('o')
    # df = dlt.read_stream(sample_table_name_merged).alias('o')
    if streaming: 
        df = spark.readStream.format("delta").table(f'LIVE.{sample_table_name_merged}').alias('o').filter('o.__END_AT is null')
    else: 
        df = dlt.read(f'{sample_table_name_merged}').alias('o').filter('o.__END_AT is null')
        
    if 'jsondoc_' not in df.columns:
        return df
     
    # Get json schema for table
    # TODO TO see where to resolve the latest column?
    # Maybe just from upstream: select * from entityindustry_input_merged_v1 => Latest one: sample_table_name_merged
    # table_schema_2 = df.orderBy(col("commit_time").desc()).select('jsondoc_').head()[0]
    # print('table_schema_2....')
    # print(table_schema_2)

    table_schema = get_json_schema_for_source_table(table)
    
    col_alias = []
    iterim_cols = []
    index = 0

    # jsondoc_ as base and must-have one

    index += 1

    if table in table_nested_json:
        # Might more than one field - using `.` as delimiter
        nested_json_fields = table_nested_json[table]
        nested_json_fields = OrderedDict(zip(nested_json_fields, nested_json_fields))
        for field in [i for i in nested_json_fields if i]: 
            iterim_col_alias = f'_{index}'
            col_iterim_base = F.from_json("o.jsondoc_", F.schema_of_json(table_schema))
            if field == 'self': # whether need to include jsondoc_ itself expansion or not
                iterim_col = col_iterim_base.alias(iterim_col_alias)
            else:
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
    
    # TODO: Need to see how to update the change part? Based on SCD2?
    return df.select('o.*', *iterim_cols).select('o.*', *[f'{i}.*' for i in col_alias], F.current_timestamp().alias("time")).drop('Associations_', 'jsondoc_')
    # return df.select('*', *iterim_cols)

# COMMAND ----------

# This might be used for certain pre-handling
def generate_cnx_dlt_output(table, streaming=True): 
    sample_table_name_merged = f'{table}_input_merged{version}'
    # sample_table_name_output = f'{table}_output{version}'
    sample_table_name_output = f'{table}_silver_s' if streaming else f'{table}_silver'
    tbl_extra_config = {}
    if not streaming: 
        tbl_extra_config.update({ 
            "spark_conf": {"pipelines.trigger.interval" : "5 seconds"}
        })
    @dlt.table(name=sample_table_name_output, **tbl_extra_config)
    @dlt.expect("valid_op_date", "updateddate_ IS NOT NULL or createddate_ is not null")
    def cnx_dlt_output():
        # return dlt.read_stream(sample_table_name_merged).select('*')
        # TODO To handle regular table by checking whether there is jsondoc_
        return expand_creditlens_dataframe_json_field(table, streaming=streaming)

tables_to_output = tables
# For debugging
# tables_to_output = ['entity']
# tables_to_output = []

for table in tables_to_output: 
    # generate_cnx_dlt_output(table, streaming=True)
    generate_cnx_dlt_output(table, False)

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

# COMMAND ----------



spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
@dlt.table(name=f'entityReference{version}')
def entityReference_final():
    return spark.sql("""
    SELECT DISTINCT
        entity.tenantIdentifier, 
        '2023-06-27' AS asOfDate,
        '0' AS scenarioIdentifier,
        entityIdentifier,
        entityIdentifierTax,
        obligorName,
        obligorNameAlias, 
        borrowerIdentifier,
        legalentity AS lei,
        entityIdentifierBVD,
        entityIdentifierLocal, 
        ue.fyemonth AS fiscalYearEndMonth,
        CASE WHEN entity.IndClassification = 'NAICS' THEN ei.Code else cast(null AS string) END AS primaryIndustryNAICS, -- TOREVIEW one option to get the substring
        CASE WHEN entity.IndClassification = 'SIC' THEN ei.Code else cast(null AS string) END AS primaryIndustrySIC, -- TOREVIEW one option to get the substring
        CASE WHEN entity.FirmType = 'PRV' then 'Private Firm' else CASE WHEN entity.FirmType = 'PUB' then 'Public Firm' else 'Unknown' end end AS entityType, 
        CASE WHEN length(incorporationState)>2 then substr(incorporationState,3) else incorporationState end AS incorporationState,
        incorporationCountryCode, 
        TO_DATE(incorporationDate, 'yyyy-MM-dd') AS incorporationDate,
        CASE WHEN entitytypelookup.IsValue is null then (CASE WHEN entityLegalForm is not null then 'Other' else entityLegalForm end) else entitytypelookup.IsValue end AS entityLegalForm, 
        geographyCode,
        riskCurrency,
        cast(null AS string) AS parentEntityIdentifier, -- TOREVIEW this table does not exist in olap, just setting to null for now.
        cast(null AS string) AS finalRatingGrade, cast(null AS string) AS ratingDate,
        cast(null AS decimal(28,6)) AS quantitativePD, cast(null AS decimal(28,6)) AS pdOneYear,
        cast(null AS string) AS privateFirmModelName, cast(null AS string) AS riskCalcStateOverride,
        portfolioUnitBanking,
        portfolioUnitCredit,
        primaryOfficerBanking,
        primaryOfficerCredit,
        current_timestamp() as time
    FROM
    (
        SELECT 
            pkid_ AS entityIdentifier,
            TaxId AS entityIdentifierTax,
            longname AS obligorName,
            shortname AS obligorNameAlias,
            systemid AS borrowerIdentifier,
            legalentity AS legalentity,
            sourcesystemidentifier AS entityIdentifierBVD,
            registrationnumber AS entityIdentifierLocal,
            indclassification AS IndClassification,
            firmtype AS FirmType,
            provincestateofincorporation AS incorporationState,
            countryofinc AS incorporationCountryCode,
            establishmentdate AS incorporationDate,
            entitytype AS entityLegalForm,
            countryofrisk AS geographyCode,
            currency AS riskCurrency,
            businessportfolio AS portfolioUnitBanking,
            creditportfolio AS portfolioUnitCredit, 
            primarybankingofficer AS primaryOfficerBanking,
            primarycreditofficer AS primaryOfficerCredit,
            tenant_id as tenantIdentifier, 
            time
        FROM
            LIVE.entity_silver
        WHERE
            islatestversion_ = 'true'
        AND
            isdeleted_ = 'false'
        AND
            isvisible_ = 'true'
        AND
            isvalid_ = 'true'
    ) entity
    LEFT JOIN
    (
        SELECT
            *
        FROM
            LIVE.entityindustry_silver
        WHERE
            islatestversion_ = 'true'
        AND
            isdeleted_ = 'false'
        AND
            isvisible_ = 'true'
        AND
            isvalid_ = 'true'
        AND
            IsPrimary = 'true'
    ) ei
    ON
        cast(entity.entityIdentifier AS int) = ei.fkid_entity
    -- AND
    --     entity.time >= ei.time
    -- AND
    --     entity.time <= ei.time + interval 1 hour
    LEFT JOIN
        LIVE.financial_silver f
    ON
        entity.entityIdentifier = f.fkid_entity
        AND
        f.islatestversion_ = 'true'
        AND
        f.isdeleted_ = 'false'
        AND
        f.isvisible_ = 'true'
        AND
        f.isvalid_ = 'true'
        AND
        f.Primary = 'true'
        -- AND
        -- ei.time >= f.time
        -- AND
        -- ei.time <= f.time + interval 1 hour
    LEFT JOIN
    (
        SELECT
            *
        FROM
            LIVE.upentity_silver
    ) ue
    ON
        entity.entityIdentifier = ue.entityid
    -- AND
    --     entity.time >= ue.time
    -- AND
    --     entity.time <= ue.time + interval 1 hour
    LEFT JOIN
        LIVE.lookup_customrefdata_input entitytypelookup
    ON
        entitytypelookup.MapType = 'ENTITYTYPEREFVALUE'
    AND
        entityLegalForm = entitytypelookup.ClValue
    -- AND
    --     entity.time >= entitytypelookup.time
    -- AND
    --     entity.time <= entitytypelookup.time + interval 1 hour
    WHERE
        f.financialTemplate in ('6','1231', '2156', '1483', '1131', '1410', '2035', '2201', '2054', '2196', '2199','200')
    """)
    
    # .withWatermark("time", "1 minutes").groupBy("time").count()
