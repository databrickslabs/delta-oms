---
title: "Execution"
date: 2021-08-04T14:26:55-04:00
weight: 10
draft: true
---
### Execution

**Q. How do I get started ?**

Please refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) guide

**Q. How do I add databases to be monitored by DeltaOMS ?**

You can add a database name to the DeltaOMS configuration table (by default called `sourceconfig`) 
using simple SQL `INSERT` statement.

Example:

`INSERT INTO <omsDBName>.sourceconfig VALUES('<Database Name>',false, Map('wildCardLevel','0'))`

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
The sample [notebook](/assets/OMS_Setup_STEP2.py) provides examples of how to automatically 
create jobs based on your input sources. 

**Q.  What is the process flow for adding new input sources for DeltaOMS tracking ?**
Assuming you already have DeltaOMS running on your environment,new input sources can be added by:

- Adding the new sources to the `sourceconfig` table. Example usage in [Developer Guide]({{%relref "developer_guide/_index.md" %}})
- Running the path configuration component, `com.databricks.labs.deltaoms.init.ConfigurePaths`
- If running as an always running streaming job, restart the DeltaOMS streaming job(s)
- If running as a scheduled job, new sources will be automatically picked up during subsequent runs

**Q. (Advanced) How do I add arbitrary wildcard paths to be tracked by DeltaOMS ?**

We recommend using the `sourceconfig` configuration table to set up input source tracking for DeltaOMS 
(Refer to [Developer Guide]({{%relref "developer_guide/_index.md" %}})

There could be instances where some special wildcard path (which does not fall under the `wildCardLevel` 
provided by DeltaOMS) needs to be tracked by DeltaOMS. These random wildcards can be configured directly 
on the `pathconfig` configuration table.

Example: Say, you need to configure paths like 
`dbfs:/databricks-datasets/*/*/*/*/*/_delta_log/*.json`,`dbfs:/databricks-datasets/*/*/*/*/_delta_log/*.json` 
and `dbfs:/databricks-datasets/*/*/_delta_log/*.json`. You could directly add them to the pathconfig table 
using the SQL statements

```
spark.sql(s"""INSERT INTO <omsDBName>.pathconfig VALUES (
  'dbfs:/databricks-datasets/4/',
  substring(sha1('dbfs:/databricks-datasets/4/'), 0, 7), 
 'dbfs:/databricks-datasets/*/*/*/*/*/_delta_log/*.json', 
 substring(sha1('dbfs:/databricks-datasets/*/*/*/*/*/_delta_log/*.json'), 0, 7), 
 Map('wildCardLevel','1'),false,'databricks-datasets-4',0,false,'2021-07-23T19:06:04.933+0000')
 """)
```
Once these are added , the operational metrics from tables under these wildcard location 
can be captured and processed using the regular OMS jobs.


