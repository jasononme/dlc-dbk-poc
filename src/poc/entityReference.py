# Databricks notebook source
# MAGIC %md
# MAGIC ## EntityReference Transformations
# MAGIC
# MAGIC The `entityRefence` transformations are divided into three steps with expanded explanation of each step below. From a high level view - the code demostrates the use of Python, PySpark, and SQL to run the entire data pipeline.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step one
# MAGIC First we are going to read all relevant datasets as stream as they currently stand:
# MAGIC 1. entityindustry 
# MAGIC 2. upentity
# MAGIC 3. lookup_customrefdata
# MAGIC 4. financial
# MAGIC 5. entity
# MAGIC
# MAGIC **note:**
# MAGIC I tried to use all data sources from CDC but for some it was not obvious where they came from or if they had it in CDC. For those I used Karen's provided data from `jsondatasample/input` - we'll have to discuss with Connector's Team to use CDC

# COMMAND ----------

import json

import dlt
import pyspark.sql.functions as F


@dlt.table(name="entityindustry_input")
def entityindustry():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation","s3://00140000lmbankcnx5305/databricks_poc/apps/_temp/_entityindustry/_dlt")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/jsondatasamples/input/entityindustry")
    )

@dlt.table(name="upentity_input")
def upentity():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation","s3://00140000lmbankcnx5305/databricks_poc/apps/_temp/_upentity/_dlt")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/upentity")
    )

@dlt.table(name="financial_input")
def financial():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation","s3://00140000lmbankcnx5305/databricks_poc/apps/_temp/_financial/_dlt")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/financial")
    )

@dlt.table(name="entity_input")
def entity():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation","s3://00140000lmbankcnx5305/databricks_poc/apps/_temp/_entity/_dlt")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/entity")
    )
    
@dlt.table(name="lookup_customrefdata_input")
def lookup_customrefdata():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation","s3://00140000lmbankcnx5305/databricks_poc/apps/_temp/_lookup_customrefdata/_dlt")
        .load("s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/jsondatasamples/lookup/lookup_customrefdata")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Two
# MAGIC This step has two minor steps - 
# MAGIC - First, we read the parquet into a spark data frame to get the json schema for the columns labels `jsondocs_`.
# MAGIC - Second, we'll read the tables above and run transformations to express the Json fields.
# MAGIC
# MAGIC **note:** All tables except for `lookup_customrefdata` need tranforming

# COMMAND ----------

def get_json_schema_from_parquet(s3_path: str, column_name: str) -> str:
    """ Grabs an s3_path of a parquet and column_name then returns a string to define a schema for a json"""
    return spark.read.parquet(s3_path).select(column_name).head()[0]

entity_industry_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/jsondatasamples/input/entityindustry",
    column_name="jsondoc_"
    )

upentity_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/upentity",
    column_name="jsondoc_"
    )

financial_schema = get_json_schema_from_parquet(
    s3_path="s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/financial",
    column_name="jsondoc_"
    )

entity_schema = get_json_schema_from_parquet(
    "s3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cdcsamples/entity",
    "jsondoc_"
    )

# COMMAND ----------

def get_sequential_array_columns(df, column_name: str, array_len: int) -> tuple:
    """ This is the same as defining each column we need to extract from an array:
    df = df.select("*", df["Keys"].getItem(0).alias("Keys_0"), df["Keys"].getItem(1).alias("Keys_1"), df["Keys"].getItem(2).alias("Keys_2"))
    except it"s defined by the array you declare to avoid long arrays being declared.
    """
    return tuple(df[column_name].getItem(i).alias(f"{column_name}_{i}") for i in range(array_len))

@dlt.table
def entityindustry_silver():
    df = (
        dlt.read_stream("entityindustry_input")
        .select("*", F.from_json("jsondoc_", F.schema_of_json(entity_industry_schema)).alias("jsondoc_2"))
        .select("*", "jsondoc_2.*")
        .select("*", "IndustryCode.*")
    )
    keys_cols = get_sequential_array_columns(df=df, column_name="Keys", array_len=3)
    df = df.select("*", *keys_cols)
    return df

@dlt.table
def upentity_silver():
    return (
        dlt.read_stream("upentity_input")
        .select("*", F.from_json("jsondoc_", F.schema_of_json(upentity_schema)).alias("jsondoc_2"))
        .select("*", "jsondoc_2.Data", "jsondoc_2.FinancialId", "jsondoc_2.UserId", "jsondoc_2.moduleId_")
        .select("*", "Data.*")
    )

@dlt.table
def financial_silver():
    df = (
        dlt.read_stream("financial_input")
        .select("*", F.from_json("jsondoc_", F.schema_of_json(financial_schema)).alias("jsondoc_2"))
        .select("*", "jsondoc_2.*")
    )
    associations_cols = get_sequential_array_columns(df=df, column_name="Associations_", array_len=19)
    df = df.select("*", *associations_cols)
    return df

@dlt.table
def entity_silver():
    df = (
        dlt.read_stream("entity_input")
        .select("*", F.from_json("jsondoc_", F.schema_of_json(entity_schema)).alias("jsondoc_2"))
        .select("*", "jsondoc_2.*")
    )
    associations_cols = get_sequential_array_columns(df=df, column_name="Associations_", array_len=52)
    df = df.select("*", *associations_cols)
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Three (The Final Step):
# MAGIC Adjust and run the Dremo SQL.
# MAGIC  

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
@dlt.table
def entityReference_gold():
    return spark.sql("""
    SELECT DISTINCT
        'banka' AS tenantIdentifier, 
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
        primaryOfficerCredit
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
            primarycreditofficer AS primaryOfficerCredit
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
    LEFT JOIN
    (
        SELECT
            *
        FROM
            LIVE.upentity_silver
    ) ue
    ON
        entity.entityIdentifier = ue.entityid
    LEFT JOIN
        LIVE.lookup_customrefdata_input entitytypelookup
    ON
        entitytypelookup.MapType = 'ENTITYTYPEREFVALUE'
    AND
        entityLegalForm = entitytypelookup.ClValue
    WHERE
        f.financialTemplate in ('6','1231', '2156', '1483', '1131', '1410', '2035', '2201', '2054', '2196', '2199','200')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Original SQL from Dremio
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC SELECT DISTINCT
# MAGIC     '0014000000NXtS8AAL' AS tenantIdentifier, 
# MAGIC     '2023-06-27' AS "asOfDate",
# MAGIC     '0' AS scenarioIdentifier,
# MAGIC     entityIdentifier,
# MAGIC     entityIdentifierTax,
# MAGIC     obligorName,
# MAGIC     obligorNameAlias, 
# MAGIC     borrowerIdentifier,
# MAGIC     legalentity AS lei,
# MAGIC     entityIdentifierBVD,
# MAGIC     entityIdentifierLocal, 
# MAGIC     ue.jsondoc_Data_fyemonth AS fiscalYearEndMonth,
# MAGIC     CASE WHEN entity.IndClassification = 'NAICS' THEN jsondoc_IndustryCode_Code else cast(null AS varchar) END AS primaryIndustryNAICS, -- TOREVIEW one option to get the substring
# MAGIC     CASE WHEN entity.IndClassification = 'SIC' THEN jsondoc_IndustryCode_Code else cast(null AS varchar) END AS primaryIndustrySIC, -- TOREVIEW one option to get the substring
# MAGIC     CASE WHEN entity.FirmType = 'PRV' then 'Private Firm' else CASE WHEN entity.FirmType = 'PUB' then 'Public Firm' else 'Unknown' end end AS entityType, 
# MAGIC     CASE WHEN length(incorporationState)>2 then substr(incorporationState,3) else incorporationState end AS incorporationState,
# MAGIC     incorporationCountryCode, 
# MAGIC     TO_DATE(incorporationDate, 'YYYY-MM-DD', 1) AS incorporationDate,
# MAGIC     CASE WHEN entitytypelookup."IsValue" is null then (CASE WHEN entityLegalForm is not null then 'Other' else entityLegalForm end) else entitytypelookup."IsValue" end AS entityLegalForm, 
# MAGIC     geographyCode, riskCurrency,
# MAGIC     cast(null AS varchar) AS parentEntityIdentifier, -- TOREVIEW this table does not exist in olap, just setting to null for now.
# MAGIC     cast(null AS varchar) AS finalRatingGrade, cast(null AS varchar) AS ratingDate,
# MAGIC     cast(null AS decimal(28,6)) AS quantitativePD, cast(null AS decimal(28,6)) AS pdOneYear,
# MAGIC     cast(null AS varchar) AS privateFirmModelName, cast(null AS varchar) AS riskCalcStateOverride,
# MAGIC     portfolioUnitBanking,
# MAGIC     portfolioUnitCredit,
# MAGIC     primaryOfficerBanking,
# MAGIC     primaryOfficerCredit
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT 
# MAGIC         en.pkid_ AS entityIdentifier,
# MAGIC         en.jsondoc_TaxId AS entityIdentifierTax,
# MAGIC         en.jsondoc_longname AS obligorName,
# MAGIC         en.jsondoc_shortname AS obligorNameAlias,
# MAGIC         en.jsondoc_systemid AS borrowerIdentifier,
# MAGIC         en.jsondoc_legalentity AS legalentity,
# MAGIC         en.jsondoc_sourcesystemidentifier AS entityIdentifierBVD,
# MAGIC         en.jsondoc_registrationnumber AS entityIdentifierLocal,
# MAGIC         en.jsondoc_indclassification AS IndClassification,
# MAGIC         en.jsondoc_firmtype AS FirmType,
# MAGIC         en.jsondoc_provincestateofincorporation AS incorporationState,
# MAGIC         en.jsondoc_countryofinc AS incorporationCountryCode,
# MAGIC         --cast(date_part('year', en.establishmentdate) AS int) AS Year_Founded,  -- TOREVIEW Removed No CDD Mapping
# MAGIC         en.jsondoc_establishmentdate AS incorporationDate,
# MAGIC         en.jsondoc_entitytype AS entityLegalForm,
# MAGIC         en.jsondoc_countryofrisk AS geographyCode,
# MAGIC         en.jsondoc_currency AS riskCurrency,
# MAGIC         en.jsondoc_businessportfolio AS portfolioUnitBanking,
# MAGIC         en.jsondoc_creditportfolio AS portfolioUnitCredit, 
# MAGIC         en.jsondoc_primarybankingofficer AS primaryOfficerBanking,
# MAGIC         en.jsondoc_primarycreditofficer AS primaryOfficerCredit
# MAGIC     FROM
# MAGIC         "moodys-ds-data"."uswe2-ds-landing-internal-301".connector.jsondatasamples.output.entity en
# MAGIC     WHERE 
# MAGIC         en.tenantIdentifier = '0014000000NXtS8AAL'
# MAGIC     AND
# MAGIC         en.islatestversion_ = 'true'
# MAGIC     AND en.isdeleted_ = 'false'
# MAGIC     AND en.isvisible_ = 'true'
# MAGIC     AND en.isvalid_ = 'true'
# MAGIC ) entity
# MAGIC LEFT JOIN
# MAGIC (
# MAGIC     SELECT
# MAGIC         *
# MAGIC     FROM "moodys-ds-data"."uswe2-ds-landing-internal-301".connector.jsondatasamples.output.entityindustry
# MAGIC     WHERE
# MAGIC         tenantIdentifier = '0014000000NXtS8AAL'
# MAGIC     AND
# MAGIC         islatestversion_ = 'true'
# MAGIC     AND
# MAGIC         isdeleted_ = 'false'
# MAGIC     AND
# MAGIC         isvisible_ = 'true'
# MAGIC     AND
# MAGIC         isvalid_ = 'true'
# MAGIC     AND
# MAGIC         jsondoc_IsPrimary = 'true'
# MAGIC ) ei 
# MAGIC ON
# MAGIC     cast(entity.entityIdentifier AS int) = ei.fkid_entity
# MAGIC LEFT JOIN
# MAGIC     "moodys-ds-data"."uswe2-ds-landing-internal-301".connector.jsondatasamples.output.financial f
# MAGIC ON
# MAGIC     f.tenantIdentifier = '0014000000NXtS8AAL'
# MAGIC     AND
# MAGIC     entity.entityIdentifier = f.fkid_entity
# MAGIC     AND
# MAGIC     f.islatestversion_ = 'true'
# MAGIC     AND
# MAGIC     f.isdeleted_ = 'false'
# MAGIC     AND
# MAGIC     f.isvisible_ = 'true'
# MAGIC     AND
# MAGIC     f.isvalid_ = 'true'
# MAGIC     AND
# MAGIC     f."jsondoc_Primary" = 'true'
# MAGIC LEFT JOIN
# MAGIC (
# MAGIC     SELECT
# MAGIC         *
# MAGIC     FROM
# MAGIC         "moodys-ds-data"."uswe2-ds-landing-internal-301".connector.jsondatasamples.output.upentity
# MAGIC     WHERE
# MAGIC         tenantIdentifier = '0014000000NXtS8AAL'
# MAGIC ) ue
# MAGIC ON
# MAGIC     entity.entityIdentifier = ue.jsondoc_entityid
# MAGIC LEFT JOIN
# MAGIC     "moodys-ds-data"."uswe2-ds-landing-internal-301".connector.jsondatasamples.lookup.lookup_customrefdata entitytypelookup
# MAGIC ON
# MAGIC     entitytypelookup."MapType" = 'ENTITYTYPEREFVALUE'
# MAGIC AND
# MAGIC     entityLegalForm = entitytypelookup."ClValue"
# MAGIC AND
# MAGIC     entitytypelookup.tenantIdentifier = '0014000000NXtS8AAL'
# MAGIC WHERE
# MAGIC     f.jsondoc_financialTemplate in ('6','1231', '2156', '1483', '1131', '1410', '2035', '2201', '2054', '2196', '2199','200')
# MAGIC ;
# MAGIC ```
