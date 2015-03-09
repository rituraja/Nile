#Nile - eCommerce Sales Analytics
Get real-time insights into sales data. Screencast can be found [Here](http://youtu.be/DOC5i-CWdSU).

##Introduction
It is a platform to analyze sales data of an Amazon like online retail business. It allows to slice & dice sales data to get historical macro view and realtime pulse of the state of the business.

##Data Model
![alt tag](https://github.com/rituraja/Nile/blob/master/datamodel.png "Data Model of Nile")

##Architecture
It is inspired by the lambda architecture.

![alt tag](https://github.com/rituraja/Nile/blob/master/pipeline.png "Architecture of Nile")

###Ingestion
Data inputs are generated (product catalog & sales order) by a Python script.
A producer is feeding sales data to Kafka & which is consumed by both the batch and realtime system.
- Multiline record to tsv (Amazon catalog imported from [Here](http://snap.stanford.edu/data/amazon-meta.html) )
- Kafka reliably chunks out messages for batch storage and speed layer
- Decouple transaction and analytical system


###Batch Layer
It is predominantly implemented in Hive. It is supplemented by MR jobs written in Java. The output of the pipeline is stored in HBase. Since the sales order data size is going to be much larger than catalog, it is performant to replicate the product catalog across all the sales order mapper.


###Speed/Realtime Layer
The pipeline reads off of the same Kafka topic as the batch. It is implemented as Storm topology in Java. It writes the output to HBase tables for the serving layer. Since the topology is maintaining aggregations it is important to keep the concurrency safe and avoid contention on the output rows. This is achieved by field grouping on output key.
![alt tag](https://github.com/rituraja/Nile/blob/master/topology.png "Storm Topology")

###Web Interface
It is implemented using Flask Framework and Highcharts.
Flask connects to Hbase using HappyBase
![alt tag](https://github.com/rituraja/Nile/blob/master/NileDashboard.png "Nile - Dashboard")

##Use-Cases
- Record historical sales data
- Daily/Monthly volumes by category
- Month to month product revenue comparative trend 
- Product popularity by location (zipcode / region)
- Real-time view of product sale by minute 


