# Databricks notebook source
import json

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
# @dlt.view(name=f'entityReferenceUpdate')
spark.sql("""
    CREATE OR REPLACE VIEW banking_da_db.0014000000nxts8.entityReferenceView AS
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
        array_max(array(entity.time, ei.time, f.time, ue.time)) as commit_time
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
            banking_da_db.0014000000nxts8_silver.entity_silver
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
            banking_da_db.0014000000nxts8_silver.entityindustry_silver
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
        banking_da_db.0014000000nxts8_silver.financial_silver f
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
            entityid, 
            FIRST_VALUE(fyemonth) OVER (PARTITION BY entityid ORDER BY time DESC) as fyemonth, 
            FIRST_VALUE(time) OVER (PARTITION BY entityid ORDER BY time DESC) as time
        FROM
            banking_da_db.0014000000nxts8_silver.upentity_silver
    ) ue
    ON
        entity.entityIdentifier = ue.entityid
    -- AND
    --     entity.time >= ue.time
    -- AND
    --     entity.time <= ue.time + interval 1 hour
    LEFT JOIN
        banking_da_db.0014000000nxts8_silver.lookup_customrefdata_input entitytypelookup
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

def entityReference_final():
    return spark.sql("select * from banking_da_db.0014000000nxts8.entityReferenceView")

# COMMAND ----------

data_file_location_root = 's3://uswe2-ds-apps-internal-301/databricks_poc/connector-samples/cl_cdc/20230629/'

sourceDF = entityReference_final()
sourceDF.createOrReplaceTempView("entityReferenceView")

entityReference_table = spark.sql(
    """
    CREATE TABLE IF NOT EXISTS banking_da_db.0014000000nxts8.entityReference_v2 LIKE banking_da_db.0014000000nxts8.entityReferenceView USING DELTA
    """)

# entityReference_table = (DeltaTable
# .createIfNotExists(spark)
# .tableName("banking_da_db.0014000000nxts8.entityReference_v2")
# .addColumn("entityIdentifier", "STRING")
# .execute())

entityReference_table = (DeltaTable.forName(spark, "banking_da_db.0014000000nxts8.entityReference_v2"))

out = entityReference_table.alias('t').merge(
    sourceDF.alias('s'), "s.entityIdentifier = t.entityIdentifier"
).whenMatchedUpdateAll('s.commit_time > t.commit_time').whenNotMatchedInsertAll().execute()

display(out)

# display(spark.sql("select * from banking_da_db.0014000000nxts8.entityreference_v1"))
