#Nile - eCommerce Sales Analytics
Get real-time insights into sales data

### Introduction
It is a plateform to analyze sales data of a Amazon like online retail buisness. It allows to slice & dice sales data to get historical macro view and realtime pulse of the state of the buisness.

Architecture
It is inspired by the lambda architecture.

![alt tag](https://github.com/rituraja/Nile/blob/master/pipeine.png "Architecture of Nile")

Ingestion
Data inputs are generated (product catalog & sales order) by a Python script.
A producer is feeding sales data to Kafka & which is consumed by both the batch and realtime system.

Batch Layer
It is predominantly implemented in Hive. It is supplemented by MR jobs written in Java. The output of the pipeline is stored in HBase. Since the sales order data size is going to be much larger than catalog, it is performant to replicate the product catalog across all the sales order mapper.

Realtime Layer
The pipeline reads off of the same Kafka topic as the batch. It is implemented as Storm topology in Java. It writes the output to HBase tables for the serving layer. Since the topology is maintaining aggregations it is important to keep the concurrency safe and avoid contention on the output rows. This is acheived by fieldgrouping on output key.

Web Interface
It is implemented using Flask Framework and Highcharts.
![alt tag](https://github.com/rituraja/Nile/blob/master/NileDashboard.png "Nile - Dashboard")

