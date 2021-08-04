---
title: "Setup"
date: 2021-08-04T14:25:26-04:00
weight: 15
draft: true
---
## Setup

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
Next, we will create couple of databricks jobs to stream the delta logs from the tracked tables 
and also to process the data for further analytics.

- Import and Open the DeltaOMS Setup notebook [STEP 2](/assets/OMS_Setup_STEP2.py) into 
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

Refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}}) for more details on multiple stream approach 
for DeltaOMS ingestion.