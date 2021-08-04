-- Databricks notebook source
-- MAGIC %md # Data Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 frequently changing tables (in last X days/hours)
SELECT path, count(DISTINCT commit_version) as commit_count
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>
GROUP BY path
ORDER BY commit_count DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Data Size changes of table(s) and database(s) over time
SELECT data_path,commit_ts,commit_version,
       sum(add_file.size) as sizeInBytes
FROM <OMS DB NAME>.<ACTION_SNAPSHOT_TABLE_NAME>
WHERE data_path = "<TABLE_DATA_PATH>"
GROUP BY data_path,commit_ts, commit_version
ORDER BY commit_version

-- COMMAND ----------

-- DBTITLE 1,Data change Operations for a table over time
SELECT commit_ts,operation, count(1) as operationCount
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>
WHERE path="<TABLE_DATA_PATH>"
GROUP BY commit_ts,operation
ORDER BY commit_ts

-- COMMAND ----------

-- MAGIC %md # User Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 active users
SELECT userName, count(1) as commit_counts
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME> 
WHERE userName IS NOT NULL
GROUP BY userName
ORDER BY commit_counts DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,List of Tables updated by users
SELECT *
FROM
(SELECT DISTINCT userName, path
 FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>  
 WHERE userName IS NOT NULL
   AND (clusterid IS NOT NULL or notebook.notebookId IS NOT NULL)
) a
JOIN
(SELECT userName, count(1) as commit_counts
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME> 
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
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME> 
WHERE operation = "OPTIMIZE"
GROUP BY path
ORDER BY last_optimize_ts

-- COMMAND ----------

-- DBTITLE 1,Top 10 Most Fragmented tables
SELECT path,sum(operationMetrics.numOutputBytes)/sum(operationMetrics.numFiles) as meanFileSizeinBytes
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME> 
WHERE operation="WRITE" AND operationMetrics.numFiles > 1
GROUP BY path
ORDER BY meanFileSizeinBytes ASC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md # Execution Characteristics

-- COMMAND ----------

-- DBTITLE 1,Top 10 active WRITE/UPDATE clusters
SELECT clusterId,count(1) as commit_count
from <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>  
WHERE clusterId IS NOT NULL
GROUP BY clusterId
ORDER BY commit_count DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Most frequently changed tables for a cluster
SELECT path,min(commit_ts) as first_change_ts, max(commit_ts) as last_change_ts, count(1) as num_changes
FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>
WHERE clusterId="<CLUSTER ID>"
GROUP BY path
LIMIT 10

-- COMMAND ----------

-- MAGIC %md # Auditing Information

-- COMMAND ----------

-- DBTITLE 1,Current Size , Last Updates Timestamp and Counts for All Lakehouse Tables
SELECT data_path as tablePath,max(struct(commit_version, commit_ts, sizeInBytes, numRecords)) as CurrentVersionDetails
FROM <OMS DB NAME>.<ACTION_SNAPSHOT_TABLE_NAME>
GROUP BY data_path 
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
-- MAGIC   FROM <OMS DB NAME>.<COMMIT_SNAPSHOT_TABLE_NAME>
-- MAGIC   WHERE operation="OPTIMIZE"
-- MAGIC   AND operationParameters.zOrderBy is not NULL
-- MAGIC   AND operationParameters.zOrderBy <> "[]"
-- MAGIC ) rct
-- MAGIC WHERE rank = 1

-- COMMAND ----------

-- DBTITLE 1,Gather Partitioning information for Delta tables
SELECT DISTINCT path,metadata.partitionColumns 
FROM <OMS DB NAME>.<RAW_ACTIONS_TABLE_NAME>
WHERE metadata IS NOT NULL 
AND size(metadata.partitionColumns) > 0)
