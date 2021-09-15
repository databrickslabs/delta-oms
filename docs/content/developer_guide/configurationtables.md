---
title: "Configuration Tables"
date: 2021-08-04T14:27:39-04:00
weight: 5
draft: false
---

### Source Config

Default Name :  `sourceconfig`

Table used for adding databases/paths/individual table to be tracked by DeltaOMS

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| path     | String | Path to the Delta object to be tracked by DeltaOMS. <br> Could be a database (all tables will be included), directory, individual table or specific path |
| skipProcessing | Boolean | Flag to exclude processing a row |
| parameters | Map<String,String> | Placeholder for dynamic parameters to be passed to DeltaOMS (supports easy future expansion). <br><br> Currently , the only required parameter is `wildCardLevel` |

Typical usage for adding input source configuration :
```$sql
-- Adding a Database. OMS will discover all Delta tables in the database. 
-- ConfigurePaths process could take longer depending on number of tables in the database.
INSERT INTO <OMSDBNAME>.sourceconfig values('<databaseName>', false, Map('wildCardLevel','0'))

-- Adding an entire directory. OMS will discover all Delta tables underneath the directory recursively. 
-- The `**` is required after the directory name. 
-- ConfigurePaths process could take longer depending on number of nested delta paths under the directory.
INSERT INTO <OMSDBNAME>.sourceconfig values('<dirName/**>', false, Map('wildCardLevel','0'))

-- Adding a table
INSERT INTO <OMSDBNAME>.sourceconfig values('<fully qualified table name>', false, Map('wildCardLevel','0'))

-- Adding an individual table path
INSERT INTO <OMSDBNAME>.sourceconfig values('<Full table path on storage>', false, Map('wildCardLevel','0'))
```

#### WildCard Level Configuration

The wildcard level setting for a path determines how the path is modified into a wildcard path 
internally and used by DeltaOMS during ingestion.

| Param Value |  Description | Example |
| :-----------: | :----------- | ------ |
| `Map('wildCardLevel','0')` | Tracks all tables in a database path | `dbfs:/user/hive/warehouse/sample.db/table1` -> `dbfs:/user/hive/warehouse/sample.db/*/_delta_log/*.json` |
| `Map('wildCardLevel','1')` | Tracks multiple database under a path | `dbfs:/user/hive/warehouse/sample.db/table1` -> `dbfs:/user/hive/warehouse/*/*/_delta_log/*.json` |
| `Map('wildCardLevel','-1')` | Tracks only the individual path | `dbfs:/user/hive/warehouse/sample.db/table1` -> `dbfs:/user/hive/warehouse/sample.db/table1/_delta_log/*.json` |

### Path Config

Default Name :  `pathconfig`

Internal table that maintains the configuration for each individual Delta table path. 
This is populated by executing the `com.databricks.labs.deltaoms.init.ConfigurePaths.main` process

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| path     | String | Path to a individual Delta table to be tracked by DeltaOMS |
| puid | String | Unique identifier for the table `path` . Used for partitioning of data inside DeltaOMS |
| wildCardPath | String | Wildcard path representation from the individual table `path` . Used for generating optimized read streams. <br> Behaviour controlled through the `wildCardLevel` parameter |
| wuid | String | Unique identifier for wildcard `path`. Used for uniquely identifying the streams |
| qualifiedName | String | Fully Qualified name of a table (if defined in the metastore). For tables not defined in metastore, defaults to the table path  |
| parameters | Map<String,String> | Placeholder for dynamic parameters to be passed to DeltaOMS (supports easy future expansion). <br><br> Currently , the only required parameter is `wildCardLevel` |

### Processed History

Default Name : `processedhistory`

Internal tracking table for last version of Delta `action`s processed by DeltaOMS. Populated during OMS data processing

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| tableName     | String | Table name tracked by DeltaOMS. Eg. - `rawactions` |
| lastVersion     | Long | Last Version of Actions processed by DeltaOMS |
| update_ts     | Timestamp | Last update timestamp |

