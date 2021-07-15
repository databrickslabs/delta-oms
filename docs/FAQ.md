# Frequently Asked Questions

### General

**Q. What is Delta Operational Metrics Store ?**

Delta Operational metrics store (DeltaOMS) is a solution/framework for automated collection 
and tracking of Delta commit logs and other future operational metrics from Delta Lake, 
build a centralized repository for Delta Lake operational statistics and simplify analysis 
across the entire data lake.

The solution can be easily enabled and configured to start capturing the operational metrics into a 
centralized repository on the data lake. Once the data is collated , it would unlock the 
possibilities for gaining operational insights, creating dashboards for traceability of operations 
across the data lake through a single pane of glass and other analytical use cases.

**Q. What are the benefits of using DeltaOMS ?**

Tracking and analyzing Delta Lake operational metrics across multiple database objects requires 
building a custom solution on the Delta Lakehouse.DeltaOMS helps to automate the collection of 
operational logs from multiple Delta Lake objects, collate those into a central repository on 
the lakehouse , allow for more holistic analysis and allow presenting them through 
a single pane of glass dashboard for typical operational analytics. 
This simplifies the process for users looking to gain insights into their Delta Lakehouse table operations.

**Q. What typical operational insights would I get from the solution ?**

DeltaOMS centralized repository provides interfaces for custom analysis on the Delta Lake 
operational metrics using tools like Apache Spark, Databricks SQL Analytics etc. 

For example, it could answer questions like :

- What are the most frequent WRITE operations across my data lake ?
- Which are the expensive WRITE operations on my lake ?
- How many WRITE operations were run on my Data Lake in the last hour ?
- What is the average duration of WRITE operations on my database Delta tables ?
- Which of the WRITE operations involve inserting the largest amount of data ?
- Which are the top WRITE heavy databases in my data lake ?
- Track File I/O ( bytes written, number of writes etc.) across my entire data lake 
- Tracking growth of data size, commit frequency etc. over time for tables/databases
- Track changes over time for Delta operations/data
- Did the delete operations for GDPR compliance go through and what changes it made to the filesystem ?
- And many more ...

**Q. Who should use this feature ?**

Data Engineering teams, Data Lake Admins and Operational Analysts would be able to 
manage and use this feature for operational insights on the Delta Lake. 

**Q. How will I be charged ?**

This solution is fully deployed in the users environment. The automated jobs for the framework 
will run on the users Databricks environment.Depending on the configuration set by the users 
(for example, update frequency of the audit logs, number of databases/delta path enabled etc.), 
the cost of the automated jobs and associated storage cost will vary. 

### Execution

**Q. How do I get started ?**

Please refer to the [Getting Started](./GETTING%20STARTED.md) guide

**Q. How do I add databases to be monitored by DeltaOMS ?**

You can add a database name to the DeltaOMS configuration table (by default called `sourceconfig`) 
using simple SQL `INSERT` statement.

Example:

`INSERT INTO <omsDBName>.sourceconfig VALUES('<Database Name>',false, Map('wildCardLevel','0'))`

For more details on the parameters, refer to [Getting Started](./GETTING%20STARTED.md) 
and [Developer](./DEVELOPER.md) Guide 

**Q. What components will to be deployed for DeltaOMS ?**

DeltaOMS deploys two primary components. One for ingestion of the delta logs from the configured 
table. This is a streaming component and can be run either or a schedule or always running 
streaming job depending on your SLA requirements for retrieving operational metrics.

The other component is a batch component for processing and enriching of the delta actions 
into different OMS specific tables.

**Q. How many ingestion jobs we need to run ?**

DeltaOMS supports 50 streams by default for each Databricks jobs. These values are configurable 
through command line parameters `--startingStream` and `--endingStream`, default 1 and 50 respectively.

We recommend setting up your jobs to support groups of 40-50 stream / wildcard paths. For example,
you have 75 unique wildcard paths to process, we recomend creating 2 Databricks jobs for DeltaOMS ingestion. 
The sample [notebook](../scripts/job/OMS_Setup_STEP2.py) provides examples of how to automatically 
create jobs based on your input sources. 


