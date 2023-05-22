---
title: "General"
date: 2021-08-04T14:50:11-04:00
weight: 5
draft: false
---

**Q. What is Delta Operational Metrics Store ?**

Delta Operational metrics store (DeltaOMS) is a solution/framework for automated collection 
and tracking of Delta commit/transaction logs and associated operational metrics from Delta Lake, 
build a centralized repository for Delta Lake operational statistics and simplify analysis 
across the entire data lake.

The solution can be easily enabled and configured to start capturing the operational metrics into a 
centralized repository on the data lake. Once the transaction logs are collected in a 
centralized database , it enables gaining operational insights, creating dashboards for traceability of operations 
across the data lake through a single pane of glass and also other analytical use cases.

**Q. What are the benefits of using DeltaOMS ?**

Tracking and analyzing Delta Lake transaction logs and operational metrics across multiple 
database objects requires building a custom solution on the Delta Lakehouse.
DeltaOMS helps to automate the collection of Delta transaction logs from multiple Delta Lake databases/tables 
into a central repository on the lakehouse. This allows execution of more holistic analysis and 
presentation through a single pane of glass dashboard for typical operational analytics. 
This simplifies the process for users looking to gain insights into their Delta Lakehouse table operations.

**Q. What typical operational insights would I get from the solution ?**

DeltaOMS centralized repository provides interfaces for custom analysis on the Delta Lake 
transaction logs and operational metrics using tools like Apache Spark, Databricks SQL etc. 

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

**Q Can I run this solution on non-Databricks environment ?**

This project is distributed under Databricks license and cannot be used outside of Databricks environment

**Q. Does it support Unity Catalog on Databricks ?**
The latest version of DeltaOMS supports Unity Catalog by allowing specifying Unity catalog databases and tables 
as Source tables to be monitored. These configuration can be executed through a Unity Ctaalog enabled cluster.
For subsequent processing of the Delta transaction logs , DeltaOMS needs to run the ingestion and processing jobs 
on non Unity Catalog cluster and depends on the instance profile associated with a Databricks cluster to function.

**Q. How will I be charged ?**

This solution is fully deployed in the users Databricks or Spark environment. The jobs for the framework 
will run on the execution environment.Depending on the configuration set by the users 
(for example, update frequency of the audit logs, number of databases/delta path enabled, number of transactions ingested etc.), 
the cost of the automated jobs and associated storage cost will vary. 

We ran few simple ingestion benchmarks on an AWS based Databricks cluster :

|                                   | Xtra Small |  Small | Medium |  Large |
| --------------------------------- |------------|--------|--------|--------|
| Initial Txns                      | 100000     | 87000  | 76400  | 27500  |
| Avg Txns Size                     | ~1 Kb      | ~500 Kb| ~1 MB  | ~2.5 MB|
| Approx Total Txns Size            | ~100 Mb    | ~44 GB | ~76 GB | ~70 GB |
| Cluster Config<br>*- Workers*<br>*- Driver*<br>*- DB Runtime*| <br>**(5) i3.2xl** - 305 GB Mem , 40 Cores <br>**i3.xl** - 61 GB Mem, 8 Cores <br> **DB Runtime** - 11.2 | <br>**(5) i3.4xl** - 610 GB Mem , 80 Cores <br>**i3.2xl** - 61 GB Mem, 8 Cores <br> **DB Runtime** - 11.2 | <br>**(5) i3.4xl** - 610 GB Mem , 80 Cores <br>**i3.2xl** - 61 GB Mem, 8 Cores <br> **DB Runtime** - 11.2  | <br>**(5) i3.4xl** - 610 GB Mem , 80 Cores <br>**i3.2xl** - 61 GB Mem, 8 Cores <br> **DB Runtime** - 11.2 |
| Initial Raw Ingestion Time        | ~15  mins      | ~ 50 mins  |  ~ 60 mins |   ~ 40 mins    |
| Incremental Additional Txns       | 1000       | 1000   | 1000   | 1000   |
| Incremental Raw Ingestion Time    |   ~ 1 min         |   ~ 2 min      |  ~ 3 min      |   ~ 3 mins     |
