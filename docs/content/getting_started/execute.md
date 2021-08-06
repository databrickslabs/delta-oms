---
title: "Execute"
date: 2021-08-04T14:25:26-04:00
weight: 20
draft: false
---

### Execute the DeltaOMS Jobs
You can run the jobs created in the above step to ingest and process the delta transaction 
information for the configured tables into the centralized DeltaOMS database.

By default, the `OMS_Streaming_Ingestion_*` jobs bring in the raw delta logs from the configured 
databases/tables. The `OMS_ProcessRawActions*` job formats and enrich the raw delta log data. 

For more details refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}})