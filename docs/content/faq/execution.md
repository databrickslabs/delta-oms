---
title: "Execution"
date: 2021-08-04T14:26:55-04:00
weight: 10
draft: false
---

**Q. How do I get started ?**

Please refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

**Q. How do I add databases to be monitored by DeltaOMS ?**

You can add a database name to the DeltaOMS configuration table (by default called `sourceconfig`) 
using simple SQL `INSERT` statement.

Example:

`INSERT INTO <omsCatalogName>.<omsDBName>.sourceconfig VALUES('<Database Name>',false)`

For more details on the configurations and parameters, refer to [Getting Started]({{%relref "getting_started/_index.md" %}})
and [Developer Guide]({{%relref "developer_guide/_index.md" %}})

**Q. What components will to be deployed for DeltaOMS ?**

DeltaOMS deploys two primary components. One for ingestion of the delta logs from the configured 
table. This is a streaming component and can be run either on a schedule or as an always running 
streaming job depending on your SLA requirements for retrieving operational metrics.

The other component is a batch component for processing and enriching of the delta actions 
into different OMS specific tables.

**Q. How many ingestion jobs we need to run ?**

DeltaOMS supports 50 streams by default for each Databricks jobs. These values are configurable 
through command line parameters `--startingStream` and `--endingStream`, default 1 and 50 respectively.

We recommend setting up your jobs to support groups of 40-50 stream / wildcard paths. For example,
you have 75 unique wildcard paths to process, we recommend creating 2 Databricks jobs for DeltaOMS ingestion.

**Q.  What is the process flow for adding new input sources for DeltaOMS tracking ?**
Assuming you already have DeltaOMS running on your environment,new input sources can be added by:

- Adding the new sources to the `sourceconfig` table. Example usage in [Developer Guide]({{%relref "developer_guide/_index.md" %}})
- Running the path configuration component, `com.databricks.labs.deltaoms.init.ConfigurePaths`
- If running as an always running streaming job, restart the DeltaOMS streaming job(s)
- If running as a scheduled job, new sources will be automatically picked up during subsequent runs

**Q. (Advanced) Is there an option to add all Delta tables under a path using wildcard expressions to be tracked by DeltaOMS ?**

You could use a special wildcard expression [`PATH/**`] to the `sourceconfig` table and add all Delta tables under a path for DeltaOMS tracking.

An example syntax to add such a path is :

`INSERT INTO <omsDBName>.sourceconfig VALUES('dbfs:/user/warehouse/**',false, Map('wildCardLevel','0'))`

DeltaOMS will discover all delta tables under the path `'dbfs:/user/warehouse/` and add it to the internal `pathconfig` table for tracking.
