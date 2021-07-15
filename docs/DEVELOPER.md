# DeltaOMS - Developer Guide

DeltaOMS provides "Automated Observability" for Delta Lakehouse objects. This developer guide 
assumes you have already completed the tutorial from the 
[Getting Started Guide](./GETTING%20STARTED.md).

Once you have completed the getting started , this guide will help with understanding 
inner workings of DeltaOMS


## Configuration Tables

### Source Config

Default Name :  `sourceconfig`

Table used for adding databases/paths/individual table to be tracked by DeltaOMS

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| path     | String | Path to the Delta object to be tracked by DeltaOMS. <br> Could be a database (all tables will be included), individual table or specific path |
| skipProcessing | Boolean | Flag to exclude processing a row |
| parameters | Map<String,String> | Placeholder for dynamic parameters to be passed to DeltaOMS (supports easy future expansion). <br><br> Currently , the only required parameter is `wildCardLevel` |

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

Internal tracking table for last version of Delta `action`s processed by DeltaOMS

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| tableName     | String | Table name tracked by DeltaOMS. Eg. - `rawactions` |
| lastVersion     | Long | Last Version of Actions processed by DeltaOMS |
| update_ts     | Timestamp | Last update timestamp |

## Data Tables

### Raw Actions

Default Name : `rawactions`

Stores the raw actions captured through the ingestion of the `_delta_log` json files for all tracked tables. 
The schema has the columns matching the Actions from here(https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L515) 
and the [Delta Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions) with 
the following additional fields :

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| file_name     | String | Name of the Delta log transaction `json` file. <br> Eg. - `dbfs:/user/hive/warehouse/sample.db/table1/_delta_log/00000000000000000025.json` |
| path     | String | Path to the Delta table. Eg. - `dbfs:/user/hive/warehouse/sample.db/table1` |
| puid     | String | Path Unique Identifier (Partition column)|
| commit_version     | Long | Transaction Commit Version of the data. Eg. - `25` |
| commit_ts     | Timestamp | Transaction Commit Timestamp. Eg. - `2021-06-16T18:08:20.000+0000` |
| update_ts     | Timestamp | Last update timestamp |
| commit_date     | Date | Transaction Commit Date (Partition column) |

### Commit Info Snapshots

Default Name : `commitinfosnapshots`

DeltaOMS processes the raw actions from the Delta logs and creates a separate table for the [Commit Information](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information)
The schema matches the [history schema](https://docs.databricks.com/delta/delta-utility.html#history-schema) with the following additional columns:

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| file_name     | String | Name of the Delta log transaction `json` file. <br> Eg. - `dbfs:/user/hive/warehouse/sample.db/table1/_delta_log/00000000000000000025.json` |
| path     | String | Path to the Delta table. Eg. - `dbfs:/user/hive/warehouse/sample.db/table1` |
| puid     | String | Path Unique Identifier (Partition column)|
| commit_version     | Long | Transaction Commit Version of the data. Eg. - `25` |
| commit_ts     | Timestamp | Transaction Commit Timestamp. Eg. - `2021-06-16T18:08:20.000+0000` |
| update_ts     | Timestamp | Last update timestamp |
| commit_date     | Date | Transaction Commit Date (Partition column) |
 
### Action Snapshots

Default Name : `actionsnapshots`

DeltaOMS ingests all the actions from the tracked delta transaction logs. During processing, DeltaOMS extracts
the [Add/Remove actions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file), 
reconciles the AddFile and RemoveFile actions to build the snapshots at each `commit_version` for a 
table/path and populates the action snapshots table.

This table provides the ability to query file snapshots for any tracked delta path at certain 
point in time / commit version and get the data file details.

| Column | Type | Description | 
| :-----------: | :----------- | ----------- | 
| data_path     | String | Path to the Delta table. Eg. - `dbfs:/user/hive/warehouse/sample.db/table1` |
| puid     | String | Path Unique Identifier (Partition column)|
| commit_version     | Long | Transaction Commit Version of the data. Eg. - `25` |
| commit_ts     | Timestamp | Transaction Commit Timestamp. Eg. - `2021-06-16T18:08:20.000+0000` |
| commit_date     | Date | Transaction Commit Date (Partition column) | 
| add_file | Struct | [AddFile](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file) |

## Key Components

### Ingestion

DeltaOMS uses the delta log json files as its input. Details of the Delta Protocol Json files 
can be found [here](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#file-types) 

Based on the databases, tables or individual path configured in DeltaOMS, it will fetch the 
json files under the `_delta_log` folder as streams. To prevent creation of multiple streams 
(applicable for large number of databases and streams) , the number of streams are optimized by 
creating streams on wildcard paths instead of individual paths for table(s).

For example, if a tracked table has the path as
`dbfs:/user/hive/warehouse/sample.db/table1`, DeltaOMS uses the path 
`dbfs:/user/hive/warehouse/sample.db/*/_delta_log/*.json` to read the Delta logs.

The benefits of this approach are :
- Automatically supports any additional table added to the database
- No additional streams needs to be created for each additional table, making the solution more scalable

For ways to configure the wildcard behaviour , refer to [Source Config](#source-config)

During ingestion, DeltaOMS reads the list of `wildcardpath`s from the `pathconfig` table and 
creates separate read streams for each path. OMS also maintains separate checkpoint location, pool 
and queryname for each stream. The Delta log json files are read, enriched with additional fields 
and persisted to the `rawactions` table.

The ingestion process is executed from the `com.databricks.labs.deltaoms.ingest.StreamPopulateOMS` 
object. By default, each streaming ingestion job can support upto 50 streams. If more than 50 
streams ( for example more than 50 distinct wildcard paths) would be involved , we recommend 
creating separate Databricks jobs. The `com.databricks.labs.deltaoms.ingest.StreamPopulateOMS` 
supports command line parameters `--startingStream` and `--endingStream` for setting the 
number of streams in each job.By default, these are set to 1 and 50 respectively.

### Processing

DeltaOMS processes the ingested data from `rawactions` and creates enriched / reconciled tables 
for Commit Info and Snapshots.

The `com.databricks.labs.deltaoms.process.OMSProcessRawActions` object looks for the newly added 
raw actions by utilizing the 
[Change Data Feed feature](https://docs.databricks.com/delta/delta-change-data-feed.html). 
The new actions are reconciled to get a list of data files conforming to 
 [AddFile](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file)
and [CommitInfo](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information). These are then persisted into Action Snapshots and Commit Ino Snapshots respectively. 

## Data Model

![Delta OMS ERD](./images/DeltaOMS_ERD.png)




