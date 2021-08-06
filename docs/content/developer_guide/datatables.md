---
title: "Data Tables"
date: 2021-08-04T14:27:39-04:00
weight: 10
draft: false
---
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

