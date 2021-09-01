---
title: "Setup"
date: 2021-08-04T14:25:26-04:00
weight: 15
draft: false
---

### Initialize the DeltaOMS Database

DeltaOMS can be configured through two methods :
- Command line parameters - Limited to few basic mandatory configurations
- Configuration file - Access to all the configuration options. Refer to 
   [Additional Configurations]({{%relref "getting_started/additionalconfigurations.md" %}}/) section below for full details

For this tutorial we will use the first option.Follow the below steps to initialize the 
DeltaOMS centralized Database and tables.

- Import and Open the DeltaOMS Setup notebook [STEP 1](/assets/OMS_Setup_STEP1.scala) into 
  your Databricks environment
- Modify the value of the variables `omsBaseLocation`, `omsDBName`, 
`omsCheckpointSuffix`, `omsCheckpointBase` as appropriate for your environment

  | Variable | Description |
  | :-----------: | :----------- |
  | omsBaseLocation | Base location/path of the OMS Database on the Delta Lakehouse |
  | omsDBName | DeltaOMS Database Name. This is the centralized database with all the Delta logs |
  | omsCheckpointBase | DeltaOMS ingestion is a streaming process.This defines the Base path for the checkpoints |
  | omsCheckpointSuffix | Suffix to be added to the checkpoint path (Helps in making the path unique) |

- Attach the DeltaOMS jar (procured from your Databricks representative or Maven) to a running cluster
- Attach the notebook to the cluster and start executing the cells
- Execute `com.databricks.labs.deltaoms.init.InitializeOMS.main` method to create the OMS DB and tables.
- Validate the DeltaOMS database and tables were created

### Configure Delta Lakehouse objects for DeltaOMS tracking

Next, we will add few input sources (existing Delta databases or tables) to be tracked by DeltaOMS.
This is done using the same notebook.

- Add the names of few databases you want to track via DeltaOMS to the `sourceconfig` table in the DeltaOMD DB. 
  This is done by using a simple SQL `INSERT` statement: 
  
  `INSERT INTO <omsDBName>.sourceconfig VALUES('<Database Name>',false, Map('wildCardLevel','0'))`
   
   Refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}}) for more details on the tables.
   
- Configure the internal DeltaOMS configuration tables by executing 
  `com.databricks.labs.deltaoms.init.ConfigurePaths.main`. 
  This will populate the internal configuration table `pathconfig` with the detailed path 
  information for all delta tables under the database
  
### Create Databricks Jobs
Next, we will create couple of databricks jobs for executing the solution. The first Databricks job
will stream ingest the delta logs from the configured delta tables and persist in the `rawactions` DeltaOMS table. 
For example, you could name the job `OMSIngestion_Job`. The main configurations for the job are:

Main class : `com.databricks.labs.deltaoms.ingest.StreamPopulateOMS` 
Example Parameters : `
["--dbName=oms_test_aug31","--baseLocation=dbfs:/user/hive/warehouse/oms","--checkpointBase=dbfs:/user/hive/warehouse/oms/_checkpoints","--checkpointSuffix=_aug31_171000","--skipPathConfig","--skipInitializeOMS","--startingStream=1","--endingStream=50"]
`

Example:

![Delta OMS Streaming Ingestion Job](/images/DeltaOMS_Ingestion_Job_1.png)

The first job can also be created through a sample script provided as part of the solution. The steps to run the sample script are:
- Import and Open the DeltaOMS Setup notebook [STEP 2](/assets/OMS_Setup_STEP2.py) into 
  your Databricks environment
- Define the values for the `omsBaseLocation`, `omsDBName`, `omsCheckpointSuffix`, `omsCheckpointBase` 
- Upload the DeltaOMS jar to a cloud path of your choice and copy the full path
- Modify the provided Job Creation Json template and variables as appropriate to your environment. 
  Make sure, the correct path of the jar is reflected in the `oms_jar_location` variable
- DeltaOMS creates individual streams for each tracked path and runs multiple such streams in a
  single Databricks job. By default, it groups 50 streams into a single databricks jobs. 
  You could change the variable `num_streams_per_job` to change number of streams per job.
- Once all the parameters are updated, run the command on the notebook to create the jobs. 
  Depending on the total number of objects tracked multiple Databricks jobs could be created
- You can navigate to the `Jobs` UI to look at the created jobs

The second job will process the raw actions and organize them into Commit Info and Action snapshots for querying and further analytics.
You could name the job `OMSProcessing_Job`. The main configurations for the job are: 

Main class : `com.databricks.labs.deltaoms.process.OMSProcessRawActions` 
Example Parameters : `
["--dbName=oms_test_aug31","--baseLocation=dbfs:/user/hive/warehouse/oms"]`

Example : 
![Delta OMS Processing Job](/images/DeltaOMS_Process_Job_1.png)

Note: Instead of setting up two different Databricks jobs , you could also setup a single job 
with multiple tasks using the [Multi-task Job](https://docs.databricks.com/data-engineering/jobs/index.html) feature.

Refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}}) for more details on multiple stream approach 
for DeltaOMS ingestion and the processing job.