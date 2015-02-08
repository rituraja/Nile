#!/bin/bash

/usr/bin/hdfs dfs -mkdir -p /user/ubuntu/NileData/products
/usr/bin/hdfs dfs -put -f ~/amazon/catalog.txt /user/ubuntu/NileData/products/.
