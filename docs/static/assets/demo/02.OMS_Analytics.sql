-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Use an UC Enabled Cluster

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup and update the DeltaOMS Catalog and Schema name on Unity Catalog

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE WIDGET TEXT CatalogName DEFAULT "deltaoms_testing_may22";
-- MAGIC CREATE WIDGET TEXT SchemaName DEFAULT "testing_may22";

-- COMMAND ----------

-- MAGIC %md # Data Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 frequently changing tables (in last X days/hours)
SELECT path, count(DISTINCT commit_version) as commit_count
FROM $CatalogName.$SchemaName.commitinfosnapshots
GROUP BY path
ORDER BY commit_count DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Data Size changes of table(s) and database(s) over time
SELECT puid,data_path,commit_ts,commit_version,
       sum(add.size) as sizeInBytes
FROM $CatalogName.$SchemaName.actionsnapshots
GROUP BY puid,data_path,commit_ts, commit_version
ORDER BY puid,data_path, commit_version

-- COMMAND ----------

-- DBTITLE 1,Data change Operations for tables over time
SELECT puid,commit_ts,operation, count(1) as operationCount
FROM $CatalogName.$SchemaName.commitinfosnapshots
WHERE puid in (select distinct puid from $CatalogName.$SchemaName.commitinfosnapshots)
GROUP BY puid,commit_ts,operation
ORDER BY commit_ts

-- COMMAND ----------

-- MAGIC %md # User Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 active users
SELECT userName, count(1) as commit_counts
FROM $CatalogName.$SchemaName.commitinfosnapshots 
WHERE userName IS NOT NULL
GROUP BY userName
ORDER BY commit_counts DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,List of Tables updated by users
SELECT *
FROM
(SELECT DISTINCT userName, path
 FROM $CatalogName.$SchemaName.commitinfosnapshots 
 WHERE userName IS NOT NULL
   AND (clusterid IS NOT NULL or notebook.notebookId IS NOT NULL)
) a
JOIN
(SELECT userName, count(1) as commit_counts
FROM $CatalogName.$SchemaName.commitinfosnapshots 
WHERE userName IS NOT NULL
GROUP BY userName
ORDER BY commit_counts DESC
LIMIT 10
) b
on a.userName = b.userName

-- COMMAND ----------

-- MAGIC %md ## Performance Characteristics

-- COMMAND ----------

-- DBTITLE 1,When was OPTIMIZE last run for tables in the Lakehouse
SELECT path, max(commit_ts) as last_optimize_ts , max(commit_version) AS last_optimize_version
FROM $CatalogName.$SchemaName.commitinfosnapshots 
WHERE operation = "OPTIMIZE"
GROUP BY path
ORDER BY last_optimize_ts

-- COMMAND ----------

-- DBTITLE 1,Top 10 Most Fragmented tables
SELECT path,sum(operationMetrics.numOutputBytes)/sum(operationMetrics.numFiles) as meanFileSizeinBytes
FROM  $CatalogName.$SchemaName.commitinfosnapshots
WHERE operation="WRITE" AND operationMetrics.numFiles > 1
GROUP BY path
ORDER BY meanFileSizeinBytes ASC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md # Execution Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 active WRITE/UPDATE clusters
SELECT clusterId,count(1) as commit_count
from $CatalogName.$SchemaName.commitinfosnapshots 
WHERE clusterId IS NOT NULL
GROUP BY clusterId
ORDER BY commit_count DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Most frequently changed tables for a cluster
SELECT path,min(commit_ts) as first_change_ts, max(commit_ts) as last_change_ts, count(1) as num_changes
FROM $CatalogName.$SchemaName.commitinfosnapshots
WHERE clusterId="<CLUSTER ID>"
GROUP BY path
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Last Modified Time for tables
SELECT path,userName, last_write_activity,datediff(current_timestamp(),last_write_activity) as daysSinceWriteActivity
FROM
(SELECT path,userName, max(commit_ts) as last_write_activity
FROM $dbname.commitinfosnapshots
WHERE userName IS NOT NULL
GROUP BY path,userName)
ORDER BY daysSinceWriteActivity

-- COMMAND ----------

-- MAGIC %md # Auditing Information

-- COMMAND ----------

-- DBTITLE 1,Current Size , Last Updates Timestamp and Counts for All Lakehouse Tables
SELECT data_path as tablePath, max(struct(commit_version,commit_ts,sizeInBytes,numRecords,numOfFiles)) AS CurrentVersionDetails
FROM
(SELECT data_path,commit_ts,commit_version,
       sum(coalesce(add.size,0)) AS sizeInBytes,
       sum(coalesce(get_json_object(add.stats,"$.numRecords"),0)) AS numRecords,
       count(1) as numOfFiles
FROM $CatalogName.$SchemaName.actionsnapshots
GROUP BY data_path,commit_ts,commit_version)
GROUP BY tablePath
ORDER BY tablePath
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Find Z-order Details for tables in the Delta Lake
-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   path as tablePath,operationParameters.zOrderBy
-- MAGIC FROM 
-- MAGIC (
-- MAGIC   SELECT
-- MAGIC     path,
-- MAGIC     operationParameters,
-- MAGIC     rank() OVER (PARTITION BY path ORDER BY commit_version DESC) as rank
-- MAGIC   FROM $CatalogName.$SchemaName.commitinfosnapshots
-- MAGIC   WHERE operation="OPTIMIZE"
-- MAGIC   AND operationParameters.zOrderBy is not NULL
-- MAGIC   AND operationParameters.zOrderBy <> "[]"
-- MAGIC ) rct
-- MAGIC WHERE rank = 1

-- COMMAND ----------

-- DBTITLE 1,Gather Partitioning information for Delta tables
SELECT DISTINCT path,metadata.partitionColumns 
FROM $CatalogName.$SchemaName.rawactions
WHERE metadata IS NOT NULL 
AND size(metadata.partitionColumns) > 0

-- COMMAND ----------


