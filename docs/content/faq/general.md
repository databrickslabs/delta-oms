---
title: "General"
date: 2021-08-04T14:50:11-04:00
weight: 5
draft: false
---

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

This solution is fully deployed in the users Databricks environment. The automated jobs for the framework 
will run on the Databricks environment.Depending on the configuration set by the users 
(for example, update frequency of the audit logs, number of databases/delta path enabled, number of transactions ingested etc.), 
the cost of the automated jobs and associated storage cost will vary. 

We ran few simple benchmarks on a cluster with 12 cores , 90 GB memory (On-Demand pricing) and noticed the following:

- Initial ingestion and processing of 36,000 Delta transactions took about 12 minutes
- Subsequently, each 1000 incremental transactions got ingested and processed in about 2 minutes 
- It costs about $15 for processing 1M transactions
