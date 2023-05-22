---
title: "Setup"
date: 2021-08-04T14:25:26-04:00
weight: 15
draft: false
---

### Initialize the DeltaOMS Database

DeltaOMS is configured through Spark configurations with prefix `spark.databricks.labs.deltaoms.`

Refer to [Additional Configurations]({{%relref "getting_started/additionalconfigurations.md" %}}/) section below for more details on the available configurations.

Follow the below steps to initialize the DeltaOMS centralized Database and tables.

- Import and Open the [DeltaOMS Setup notebook](/assets/OMS_Setup.scala) into your Databricks environment
 Modify the value of the variables for catalog name, schema name, external location URl and name as appropriate for your environment

  | Variable | Description |
  | :-----------: | :----------- |
  | omsLocationUrl | Base location/path of the OMS catalog and schema on the Delta Lake. This is created as an EXTERNAL LOCATION on Unity Catalog (UC)  |
  | omsLocationName | Name of the UC EXTERNAL LOCATION for the OMS catalog and schema on the Delta Lake |
  | storageCredentialName | Storage credential name for the UC EXTERNAL LOCATION created for DeltaOMS. This is usually provided by your admin |
  | omsCatalogName | OMS Catalog Name. This is the UC Catalog where all the DeltaOMS tables will be created |
  | omsSchemaName | OMS Schema Name. This is the database where all the Delta log details will be collected in tables |

- Attach the DeltaOMS jar (as a library through Maven) to a running Unity Catalog Enabled cluster
  - Select `Install New` from the clusters `Libraries` tab
  ![Install New](/images/Library_Install_New.png)
  - In the `Install Library` window, select `Maven` and click on `Search packages`
  ![Maven](/images/Library_Install_Maven.png)
  - Select `Maven Central` from the drop-down
  ![Maven](/images/Library_Install_Maven_Central.png)
  - Search for `delta-oms` and select the latest release version
  ![Maven](/images/Library_Install_Maven_DeltaOMS.png)
  - Finally, click `Install` to install the DeltaOMS library into the cluster
- Attach the imported notebook to the cluster and start executing the cells
- Execute `com.databricks.labs.deltaoms.init.InitializeOMS.main` method to create the OMS External Location , Catalog, Schema and tables.
- Validate the DeltaOMS catalog objects like Schemas and tables were created

### Configure Delta Lakehouse objects for DeltaOMS tracking

Next, we will add few input sources (existing Delta databases or tables) to be tracked by DeltaOMS.
This is done using the same notebook.

- Add the names of few databases, catalog, wildCard path etc you want to monitor via DeltaOMS to the `sourceconfig` table.
  This is done by using a simple SQL `INSERT` statements shown as examples in the notebook 

   Refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}}) for more details on the tables.
   
- Configure the internal DeltaOMS configuration tables by executing 
  `com.databricks.labs.deltaoms.init.ConfigurePaths.main`. 
  This will populate the internal configuration table `pathconfig` with the detailed path 
  information for all monitored delta tables under the database(s)
  
### Create Databricks Jobs
Next, we will create couple of databricks jobs for executing the solution. These jobs can be created manually by following the configuration options mentioned below.

#### Ingestion Job

The first databricks job will stream ingest the delta logs from the configured delta tables and persist in the `rawactions` DeltaOMS table. 
For example, you could name the job `OMSIngestion_Job`. The main configurations for the job are:

Main class : `com.databricks.labs.deltaoms.ingest.StreamPopulateOMS` 

Example Spark Configurations to be set for the job : `
["spark.databricks.labs.deltaoms.location.url s3://deltaoms-root-bucket/deltaoms
  spark.databricks.labs.deltaoms.location.name deltaoms-external-location"
]`
Refer to [Additional Configurations]({{%relref "getting_started/additionalconfigurations.md" %}}/) for full configuration details

![Delta OMS Streaming Ingestion Job](/images/DeltaOMS_Ingestion_Job_Full.png)

#### Processing Job

The second job will process the raw actions and organize them into Commit Info and Action snapshots for querying and further analytics.
You could name the job `OMSProcessing_Job`. The main configurations for the job are: 

Main class : `com.databricks.labs.deltaoms.process.OMSProcessRawActions` 

Example Spark Configurations to be set for the job :  `
["spark.databricks.labs.deltaoms.location.url s3://deltaoms-root-bucket/deltaoms
spark.databricks.labs.deltaoms.location.name deltaoms-external-location"
]`

Example : 
![Delta OMS Processing Job](/images/DeltaOMS_Processing_Job_Full.png)

Note: Instead of setting up two different Databricks jobs , you could also setup a single Databricks Workflow 
with multiple tasks/jobs using the [Databricks Workflows](https://docs.databricks.com/workflows/index.html).

Refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}}) for more details on multiple stream approach 
for DeltaOMS ingestion and the processing job.