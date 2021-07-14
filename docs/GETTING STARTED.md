# Delta OMS - Getting Started

The following tutorial will guide you through the process for setting up the Delta Operational 
Metrics Store (DeltaOMS) on your Databricks Lakehouse environment.

You will deploy/configure the solution, configure a database/table for metrics collection, 
execute DeltaOMS to collate the metrics and finally run some 
sample analysis on the processed data.

## Prerequisites

Make sure you have the following available before proceeding :

- Access to Databricks environment with access to Delta tables/databases
- Ability to create Databricks job and run them on new cluster
- Proper access permissions to create database, tables and write data to the desired OMS location
- Procured the Delta OMS solution package from your Databricks team (includes Delta OMS jar , 
sample notebooks and documentations)
- Databricks Runtime 8.3+

## Setup

#### Initialize the DeltaOMS Database

DeltaOMS can be configured through two methods :
- Command line parameters - Limited to few basic mandatory configurations
- Configuration file - Access to all the configuration options. Refer to 
   [Additional Configurations](#Additional Configurations) section below for full details

For this tutorial we will use the first option.Follow the below steps to initialize the 
DeltaOMS centralized Database and tables.

- Import and Open the DeltaOMS Setup notebook [STEP 1](../scrips/job/OMS_Setup_STEP1.scala) into 
  your Databricks environment
- Modify the value of the variables `omsBaseLocation`, `omsDBName`, 
`omsCheckpointSuffix`, `omsCheckpointBase` as appropriate for your environment

  | Variable | Description |
  | :-----------: | :----------- |
  | omsBaseLocation | Base location/path of the OMS Database on the Delta Lakehouse |
  | omsDBName | DeltaOMS Database Name. This is the centralized database with all the Delta logs |
  | omsCheckpointBase | DeltaOMS ingestion is a streaming process.This defines the Base path for the checkpoints |
  | omsCheckpointSuffix | Suffix to be added to the checkpoint path (Helps in making the path unique) |

  Example :

- Execute `com.databricks.labs.deltaoms.init.InitializeOMS.main` method to create the OMS DB and tables.
- Validate the DeltaOMS database and tables were created

#### Configure Delta Lakehouse objects for DeltaOMS tracking

Next, we will add few input sources (existing Delta databases or tables) to be tracked by DeltaOMS.
This is done using the same notebook.

- Add the names of few databases you want to track via DeltaOMS to the `sourceconfig` table in the DeltaOMD DB. 
  This is done by using a simple SQL `INSERT` statement: 
  `INSERT INTO <omsDBName>.sourceconfig VALUES('<Database Name>',false, Map('wildCardLevel','0'))`

- Configure the internal DeltaOMS configuration tables by executing 
  `com.databricks.labs.deltaoms.init.ConfigurePaths.main`. 
  This will populate the internal configuration table `pathconfig` with the detailed path 
  information for all delta tables under the database
  
#### Create Databricks Jobs
Next, we will create couple of databricks jobs to stream the delta logs from the tracked tables 
and also to process the data for further analytics.

- Import and Open the DeltaOMS Setup notebook [STEP 2](../scrips/job/OMS_Setup_STEP2.py) into 
  your Databricks environment
- Define the values for the `omsBaseLocation`, `omsDBName`, `omsCheckpointSuffix`, `omsCheckpointBase` 
- Upload the DeltaOMS jar to a cloud path of your choice and copy the full path
- Modify the provided Job Creation Json template and variables as appropriate to your environment. 
  Make sure, the correct path of the jar is reflected in the `oms_jar_location` variable
- DeltaOMS creates individual streams for each tracked path and runs multiple such streams in a
  single Databricks job. By default, it groups 50 streams into a single databricks jobs. 
  You could change the variable `num_streams_per_job` to change number of streams per job.
- Once all the parameters are updated, run the command on the notebok to create the jobs. 
  Depending on the total number of objects tracked multiple Databricks jobs could be created
- You can navigate to the `Jobs` UI to look at the created jobs

## Execute

#### Execute the DeltaOMS Jobs
You can run the jobs created in the above step to ingest and process the delta transaction 
information for the configured tables into the centralized DeltaOMS database.

By default, the `OMS_Streaming_Ingestion_*` jobs bring in the raw delta logs from the configured 
databases/tables. The `OMS_ProcessMetrics` job formats and enrich the raw delta log data. 
[More details]()

## Analyze

#### Run analytics using sample notebooks

You can run some sample analytics on the data from the OMS database 
using the provided [notebook](./notebooks/DeltaOMS_SQL_Analytics.sql)

Modify the queries to reflect your configuration for 
`OMS DB Name` and other table names.

Executing this notebook will give you an idea on the type of analysis and data structure that can 
be utilized as part of the DeltaOMS.

You can build on top of this notebook , customize this notebook to your liking and 
create your own Analytics insights and dashboards through Databricks notebooks and/or SQL Analytics.


## Additional Configurations and Execution Options

#### Configuration

DeltaOMS can also be configured using a 
[HOCON](https://github.com/lightbend/config/blob/master/HOCON.md#hocon-human-optimized-config-object-notation) format file. 
The different configuration parameters available are described below : 

| Configuration Key | Description | Required | Example | Default Value |
| :-----------: | :----------- | ----------- | ----------- | ----------- |
| base-location      | Base location/path of the OMS Database on the Delta Lake  | Y | dbfs:/spark-warehouse/oms.db | None |
| db-name   | OMS Database Name. This is the database where all the Delta log details will be collected | Y | oms.db | None |
| checkpoint-base   | Base path for the checkpoints for OMS streaming pipeline for collecting the Delta logs for the configured tables | Y | dbfs:/_oms_checkpoints/ | None |
| checkpoint-suffix   | Suffix to be added to the checkpoint path. Useful during testing for starting off a fresh process | Y | _1234 | None |
| raw-action-table   | OMS table name for storing the raw delta logs collected from the configured tables | N | oms_raw_actions | raw_actions |
| source-config-table   | Configuration table name for setting the list of Delta Path, databases and/or tables for which the delta logs should be collected by OMS | N | oms_source_config | source_config |
| path-config-table   | Configuration table name for storing Delta path details and few related metadata for internal processing purposes by OMS | N | oms_path_config | path_config |
| processed-history-table   | Configuration table name for storing processing details for OMS ETL Pipelines. Used internally by OMS | N | oms_processed_history | processed_history |
| commit-info-snapshot-table   | Table name for storing the Delta Commit Information generated from the processed raw Delta logs for configured tables/paths | N | oms_commitinfo_snapshots | commitinfo_snapshots |
| action-snapshot-table   | Table name for storing the Delta Actions information snapshots. Generated from processing the Raw Delta logs | N | oms_action_snapshot | action_snapshot |
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

#### Execution

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

![Delta OMS Streaming Ingestion Job Task Config](./images/DeltaOMS_Ingestion_Job_1.png)

![Delta OMS Streaming Ingestion Job Spark Config](./images/DeltaOMS_Ingestion_Job_2.png)




