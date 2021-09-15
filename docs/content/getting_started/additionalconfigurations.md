---
title: "Additional Configuration"
date: 2021-08-04T14:25:26-04:00
weight: 30
draft: false
---

### Spark Configuration

The following Spark configuration (spark.conf) can be used (on the cluster or job) to configure DeltaOMS :

| Configuration Key | Description | 
| :----------- | :----------- | 
| databricks.labs.deltaoms.base.location      | Base location/path of the OMS Database on the Delta Lake  |
| databricks.labs.deltaoms.db.name   | OMS Database Name. This is the database where all the Delta log details will be collected | 
| databricks.labs.deltaoms.checkpoint.base   | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | 
| databricks.labs.deltaoms.checkpoint.suffix   | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process |


### Configuration

DeltaOMS can also be configured using a 
[HOCON](https://github.com/lightbend/config/blob/master/HOCON.md#hocon-human-optimized-config-object-notation) format file. 
The different configuration parameters available are described below : 

| Configuration Key | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- |
| base-location      | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| checkpoint-base   | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | Y | dbfs:/_oms_checkpoints/ | None |
| checkpoint-suffix   | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process | Y | _1234 | None |
| raw-action-table   | OMS table name for storing the raw delta logs collected from the configured tables | N | oms_raw_actions | rawactions |
| source-config-table   | Configuration table name for setting the list of Delta Path, databases and/or tables for which the delta logs should be collected by OMS | N | oms_source_config | sourceconfig |
| path-config-table   | Configuration table name for storing Delta path details and few related metadata for internal processing purposes by OMS | N | oms_path_config | pathconfig |
| processed-history-table   | Configuration table name for storing processing details for OMS ETL Pipelines. Used internally by OMS | N | oms_processed_history | processedhistory |
| commit-info-snapshot-table   | Table name for storing the Delta Commit Information generated from the processed raw Delta logs for configured tables/paths | N | oms_commitinfo_snapshots | commitinfosnapshots |
| action-snapshot-table   | Table name for storing the Delta Actions information snapshots. Generated from processing the Raw Delta logs | N | oms_action_snapshots | actionsnapshots |
| consolidate-wildcard-path   | Flag to enable/disable processing Delta logs using consolidated wildcard patterns extracted from the path configured for OMS | N | false | true |
| trigger-interval   | Trigger interval for processing the Delta logs from the configured tables/paths  | N | 30s | Once |
| src-database   | Comma separated list of Source database used for filtering when extracting the Delta table path information from metastore  | N | Sample_db,test_db |  |
| table-pattern   | Wildcard filtering of tables to be extracted from the metastore for configuring the Delta OMS solution | N | \*oms* | \*
| starting-stream | Starting stream number for the Ingestion Job | N | 10 | 1 |
| ending-stream | Ending stream number for the Ingestion Job | N | 30 | 50 |

A sample configuration file :

```
base-location="dbfs:/home/warehouse/oms/"
db-name="oms_sample"
raw-action-table="raw_actions"
source-config-table ="source_config"
path-config-table="path_config"
processed-history-table="processed_history"
commit-info-snapshot-table="commitinfo_snapshots"
action-snapshot-table="action_snapshots"
consolidate-wildcard-path = "true"
checkpoint-base="dbfs:/home/oms/_checkpoints"
checkpoint-suffix="_sample_123900"
trigger-interval="once"
```

You could use the above information to create a configuration file appropriate to your environment. 
The configuration file can be uploaded to DBFS or your cloud storage provider.
**Tip**: You could directly create the configuration file on DBFS from a notebook using 
[dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html) APIs

### Execution

The setup and execution of DeltaOMS can also be configured through the above configuration file 
instead of command line parameters. The configuration approach provides you access to more 
parameters within DeltaOMS.

Once the config file has been created and uploaded to a cloud storage,  either the 
earlier provided notebooks or jobs can be utilized for running the setup and execution.

On a notebook, the configuration can be provided by setting the system property 
`OMS_CONFIG_FILE`. For example, the initialization of DeltaOMS can be executed as follows :
```
System.setProperty("OMS_CONFIG_FILE", "<<Full path to the configuration file>>")
val args = Array.empty[String]
com.databricks.labs.deltaoms.init.InitializeOMS.main(args)
```
For a job, the DeltaOMS configuration file can be added to the system path by 
modifying the cluster for the job and setting the advanced Spark configuration

`spark.driver.extraJavaOptions -DOMS_CONFIG_FILE=<<FULL PATH to the OMS Configuration file>>`

Refer to screenshots below for an example:

![Delta OMS Streaming Ingestion Job Task Config](/images/DeltaOMS_Ingestion_Job_Config_1.png)

![Delta OMS Streaming Ingestion Job Spark Config](/images/DeltaOMS_Ingestion_Job_Config_2.png)

