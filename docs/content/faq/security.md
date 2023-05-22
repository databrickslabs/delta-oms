---
title: "Security"
date: 2021-08-04T14:26:55-04:00
weight: 15
draft: false
---

**Q. Would I need to give permissions to all my data for this solution to work ?**

DeltaOMS processes the [Delta transaction logs](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)
implementing the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) to capture the different operational metrics. 
These logs are stored under the folder `_delta_log` alongside your data files.
DeltaOMS only requires access to this `_delta_log` folder for the tracked data locations on a non-UC enabled cluster through Instance Profile

For more details on how DeltaOMS works please refer to the [Developer Guide]({{%relref "developer_guide/_index.md" %}})

**Q. I still feel uncomfortable sharing the `_delta_log` folder outside of my team ?**

DeltaOMS is flexible in terms of deployment and can be easily deployed as a centralized solution 
or de-centralized amongst teams depending on your organizational requirements.

For example, each team could run their own configured DeltaOMS and write the operational metrics into 
different metrics store database like `delta-oms-team1`, `delta-oms-team2` etc.

For more details on how to configured DeltaOMS, refer to the [Getting Started]({{%relref "getting_started/_index.md" %}}) 
and [Developer]({{%relref "developer_guide/_index.md" %}}) guide.

