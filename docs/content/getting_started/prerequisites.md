---
title: "Pre-Requisites"
date: 2021-08-04T14:25:26-04:00
weight: 10
draft: false
---

Make sure you have the following available before proceeding :

- Access to Databricks environment with access to Delta tables/databases defined on Unity Catalog and/or Hive Metastore
- Access to create and run notebooks on Unity Catalog and Non-Unity Catalog enabled clusters
- Ability to create Databricks job/workflows and run them on new cluster
- Proper access permissions to create catalog, schema, tables at the defined OMS external location
- Access to create [libraries](https://docs.databricks.com/libraries/index.html) on your Databricks environment. Required to attach the relevant DeltaOMS (delta-oms) libraries
- (Optional) Able to access the DeltaOMS github [repo](https://github.com/databrickslabs/delta-oms) for demo notebooks and scripts
- Databricks Runtime 11.3+ environment with unity catalog enabled
