package com.insight.nile.realtime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ProductCountWriter {
  private static final DateTimeFormatter dayfmt = ISODateTimeFormat.basicDate();
  private static final DateTimeFormatter minfmt = ISODateTimeFormat.hourMinute();

  public static void updateCount(String product_id, long timestamp, int count) {
    String day = dayfmt.print(timestamp);
    String rowKey = product_id + day;
    String colKey = minfmt.print(timestamp);
    int value = count;

    System.out.println(rowKey + " " + colKey + " " + value);

    Configuration hBaseConfig =  HBaseConfiguration.create();
    //hBaseConfig.setInt("timeout", 120000);
    String zookeeperHost = "localhost";
    hBaseConfig.set("hbase.zookeeper.quorum",zookeeperHost );
    //hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

    try {
      Connection conn = ConnectionFactory.createConnection(hBaseConfig);
      System.out.println("Got connections?");
      Table table = conn.getTable(TableName.valueOf("productCount_Min"));
      Put p = new Put(Bytes.toBytes(rowKey));
      p.add(Bytes.toBytes("min"), Bytes.toBytes(colKey), Bytes.toBytes(value));
      table.put(p);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {

    ProductCountWriter.updateCount("sd346346", System.currentTimeMillis(), 3);
  }

}
