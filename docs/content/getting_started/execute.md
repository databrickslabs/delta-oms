---
title: "Execute"
date: 2021-08-04T14:25:26-04:00
weight: 20
draft: false
---

### Execute the DeltaOMS Jobs
You can run the jobs created in the previous step to ingest and process the delta transaction 
information for the configured tables into the centralized DeltaOMS schema.

For example, the `OMSIngestion_*` job(s) bring in the raw delta logs from the configured 
databases/tables and the `OMSProcessing_*` job processes the raw delta logs to format and 
enrich them for analytics.

For more advanced details on the working of these jobs please 
refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}})