---
title: "Additional Configuration"
date: 2021-08-04T14:25:26-04:00
weight: 30
draft: false
---

### Spark Configuration

DeltaOMS uses multiple Spark configurations to control its different [components]({{%relref "developer_guide/components.md" %}}/).

DeltaOMS Spark configuration (spark.conf) details :

| Configuration Key | Description | Required | Example | Default Value | Applies to components |
| :----------- | :----------- | ----------- | ----------- | ----------- | -------------------- |
| databricks.labs.deltaoms.base.location      | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db, /tmp/spark-warehouse/oms.db | None | All |
| databricks.labs.deltaoms.db.name   | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None | All |
| databricks.labs.deltaoms.checkpoint.base  | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | Y | dbfs:/_oms_checkpoints/ | None | Ingestion |
| databricks.labs.deltaoms.checkpoint.suffix   | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process | Y | _1234 | None | Ingestion |
| databricks.labs.deltaoms.raw.action.table   | OMS table name for storing the raw delta logs collected from the configured tables | N | oms_raw_actions | rawactions | Initialization |
| databricks.labs.deltaoms.source.config.table   | Configuration table name for setting the list of Delta Path, databases and/or tables for which the delta logs should be collected by OMS | N | oms_source_config | sourceconfig | Initialization |
| databricks.labs.deltaoms.path.config.table   | Configuration table name for storing Delta path details and few related metadata for internal processing purposes by OMS | N | oms_path_config | pathconfig | Initialization |
| databricks.labs.deltaoms.processed.history.table   | Configuration table name for storing processing details for OMS ETL Pipelines. Used internally by OMS | N | oms_processed_history | processedhistory | Initialization |
| databricks.labs.deltaoms.commitinfo.snapshot.table   | Table name for storing the Delta Commit Information generated from the processed raw Delta logs for configured tables/paths | N | oms_commitinfo_snapshots | commitinfosnapshots | Initialization |
| databricks.labs.deltaoms.action.snapshot.table  | Table name for storing the Delta Actions information snapshots. Generated from processing the Raw Delta logs | N | oms_action_snapshots | actionsnapshots | Initialization |
| databricks.labs.deltaoms.consolidate.wildcard.paths   | Flag to enable/disable processing Delta logs using consolidated wildcard patterns extracted from the path configured for OMS | N | false | true | Ingestion |
| databricks.labs.deltaoms.truncate.path.config   | Truncate the internal Path Config table  | N | false | false | Configuration |
| databricks.labs.deltaoms.skip.path.config   | Skip populating the internal Path Config tables during each streaming ingestion run  | N | true | false | Ingestion |
| databricks.labs.deltaoms.skip.initialize   | Skip running DeltaOMS initialization for each run  | N | true | false | Configuration, Ingestion  |
| databricks.labs.deltaoms.trigger.interval   | Trigger interval for processing the Delta logs from the configured tables/paths  | N | 30s | AvailableNow | Ingestion |
| databricks.labs.deltaoms.starting.stream | Starting stream number for the Ingestion Job | N | 10 | 1 | Ingestion |
| databricks.labs.deltaoms.ending.stream | Ending stream number for the Ingestion Job | N | 30 | 50 | Ingestion |
| databricks.labs.deltaoms.use.autoloader | Use Autoloader for the Ingestion Job | N | false | true | Ingestion |
