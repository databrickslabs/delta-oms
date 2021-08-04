---
title: "Key Components"
date: 2021-08-04T14:27:39-04:00
weight: 20
draft: true
---

## Key Components

### Initialization

DeltaOMS provides the component `com.databricks.labs.deltaoms.init.InitializeOMS` for initializing  
the centralized OMS database. The component creates the OMS DB at the location specified by the configuration settings.
Note: This process will delete all existing data in the specified location.

| Configuration Key | Command Line Argument | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- | ----------- |
| base-location   |  --baseLocation= | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | --dbName= | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| raw-action-table  | None | OMS table name for storing the raw delta logs collected from the configured tables | N | oms_raw_actions | rawactions |
| source-config-table  | None | Configuration table name for setting the list of Delta Path, databases and/or tables for which the delta logs should be collected by OMS | N | oms_source_config | sourceconfig |
| path-config-table   | None | Configuration table name for storing Delta path details and few related metadata for internal processing purposes by OMS | N | oms_path_config | pathconfig |
| processed-history-table  | None | Configuration table name for storing processing details for OMS ETL Pipelines. Used internally by OMS | N | oms_processed_history | processedhistory |
| commit-info-snapshot-table  | None | Table name for storing the Delta Commit Information generated from the processed raw Delta logs for configured tables/paths | N | oms_commitinfo_snapshots | commitinfosnapshots |
| action-snapshot-table  | None | Table name for storing the Delta Actions information snapshots. Generated from processing the Raw Delta logs | N | oms_action_snapshots | actionsnapshots |

### Configuration

Once DeltaOMS has been initialized, and the input sources configured [Source Config](#source-config), 
the component provided by `com.databricks.labs.deltaoms.init.ConfigurePaths` is executed to 
populate the internal [path config](#path-config) tables. This component is also responsible for 
discovering delta tables under a database or under a directory (based on how the source is configured).

| Configuration Key | Command Line Argument | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- | ----------- |
| base-location   |  --baseLocation= | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | --dbName= | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| skip-initialize-oms | --skipInitializeOMS | Skip initializing DeltaOMS DB  | N | true | false |

This component should be executed whenever there are updates to the source config table.

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

| Configuration Key | Command Line Argument | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- | ----------- |
| base-location   |  --baseLocation= | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | --dbName= | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| checkpoint-base   | --checkpointBase= | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | Y | dbfs:/_oms_checkpoints/ | None |
| checkpoint-suffix  | --checkpointSuffix= | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process | Y | _1234 | None |
| trigger-interval  | None | Trigger interval for processing the Delta logs from the configured tables/paths  | N | 30s | Once |
| starting-stream | --startingStream= | Starting stream number for the Ingestion Job | N | 10 | 1 |
| ending-stream | --endingStream= | Ending stream number for the Ingestion Job | N | 30 | 50 |
| skip-path-config | --skipPathConfig | Skip updating the Path Config table from the latest Source config  | N | true | false |
| skip-initialize-oms | --skipInitializeOMS | Skip initializing DeltaOMS DB  | N | true | false |


### Processing

DeltaOMS processes the ingested data from `rawactions` and creates enriched / reconciled tables 
for Commit Info and Snapshots.

The `com.databricks.labs.deltaoms.process.OMSProcessRawActions` object looks for the newly added 
raw actions by utilizing the 
[Change Data Feed feature](https://docs.databricks.com/delta/delta-change-data-feed.html). 
The new actions are reconciled to get a list of data files conforming to 
 [AddFile](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file)
and [CommitInfo](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information). 
These are then persisted into Action Snapshots and Commit Info Snapshots respectively. 

| Configuration Key | Command Line Argument | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- | ----------- |
| base-location   |  --baseLocation= | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | --dbName= | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |

