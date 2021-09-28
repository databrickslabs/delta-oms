---
title: "Additional Configuration"
date: 2021-08-04T14:25:26-04:00
weight: 30
draft: false
---

### Spark Configuration

The following Spark configuration (spark.conf) can be used (on the cluster or job) to configure DeltaOMS :

| Configuration Key | Description | Required | Example | Default Value |
| :----------- | :----------- | ----------- | ----------- | ----------- |
| databricks.labs.deltaoms.base.location      | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| databricks.labs.deltaoms.db.name   | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| databricks.labs.deltaoms.checkpoint.base  | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | Y | dbfs:/_oms_checkpoints/ | None |
| databricks.labs.deltaoms.checkpoint.suffix   | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process | Y | _1234 | None |
| databricks.labs.deltaoms.raw.action.table   | OMS table name for storing the raw delta logs collected from the configured tables | N | oms_raw_actions | rawactions |
| databricks.labs.deltaoms.source.config.table   | Configuration table name for setting the list of Delta Path, databases and/or tables for which the delta logs should be collected by OMS | N | oms_source_config | sourceconfig |
| databricks.labs.deltaoms.path.config.table   | Configuration table name for storing Delta path details and few related metadata for internal processing purposes by OMS | N | oms_path_config | pathconfig |
| databricks.labs.deltaoms.processed.history.table   | Configuration table name for storing processing details for OMS ETL Pipelines. Used internally by OMS | N | oms_processed_history | processedhistory |
| databricks.labs.deltaoms.commitinfo.snapshot.table   | Table name for storing the Delta Commit Information generated from the processed raw Delta logs for configured tables/paths | N | oms_commitinfo_snapshots | commitinfosnapshots |
| databricks.labs.deltaoms.action.snapshot.table  | Table name for storing the Delta Actions information snapshots. Generated from processing the Raw Delta logs | N | oms_action_snapshots | actionsnapshots |
| databricks.labs.deltaoms.consolidate.wildcard.paths   | Flag to enable/disable processing Delta logs using consolidated wildcard patterns extracted from the path configured for OMS | N | false | true |
| databricks.labs.deltaoms.truncate.path.config   | Truncate the internal Path Config table  | N | false | false |
| databricks.labs.deltaoms.skip.path.config   | Skip populating the internal Path Config tables during each streaming ingestion run  | N | true | false |
| databricks.labs.deltaoms.skip.initialize   | Skip running DeltaOMS initialization for each run  | N | true | false |
| databricks.labs.deltaoms.trigger.interval   | Trigger interval for processing the Delta logs from the configured tables/paths  | N | 30s | Once |
| databricks.labs.deltaoms.src.databases   | Comma separated list of Source database used for filtering when extracting the Delta table path information from metastore  | N | Sample_db,test_db |  |
| databricks.labs.deltaoms.table.pattern   | Wildcard filtering of tables to be extracted from the metastore for configuring the Delta OMS solution | N | \*oms* | \*
| databricks.labs.deltaoms.starting.stream | Starting stream number for the Ingestion Job | N | 10 | 1 |
| databricks.labs.deltaoms.ending.stream | Ending stream number for the Ingestion Job | N | 30 | 50 |
