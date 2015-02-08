package com.insight.nile.realtime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ProductCountWriter {
  private static final DateTimeFormatter dayfmt = ISODateTimeFormat.basicDate();
  private static final DateTimeFormatter minfmt = ISODateTimeFormat
      .hourMinute();

  public static void updateCount(String product_id, long timestamp, int count) {
    String day = dayfmt.print(timestamp);
    String rowKey = product_id + day;
    String colFamily = "min";
    String colKey = minfmt.print(timestamp);
    int value = count;

    System.out.println(rowKey + " " + colKey + " " + value);

    Configuration hBaseConfig = HBaseConfiguration.create();
    // hBaseConfig.setInt("timeout", 120000);
    String zookeeperHost = "ip-172-31-3-237.us-west-1.compute.internal";
    hBaseConfig.set("hbase.zookeeper.quorum", zookeeperHost);
    // hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

    try {
      HConnection conn = HConnectionManager.createConnection(hBaseConfig);
      System.out.println("==================================Got connections?");
      HTableInterface table = conn.getTable(TableName
          .valueOf("productCount_Min"));
      Get g = new Get(Bytes.toBytes(rowKey));
      Result row = table.get(g);
      System.out.println("================================== get row");
      if (!row.isEmpty()) {
        System.out.println("================================== row not empty");
        byte[] oldValue = row.getValue(Bytes.toBytes(colFamily),
            Bytes.toBytes(colKey));
        if (oldValue != null) {
          System.out
              .println("================================== cell not empty");
          int oldCount = Bytes.toInt(oldValue);
          System.out.println("================================== old count "
              + oldCount);
          System.out.println("================================== value is "
              + value);
          value += oldCount;
        }
      }
      System.out.println("=================================new value is "
          + value);

      Put p = new Put(Bytes.toBytes(rowKey));
      p.add(Bytes.toBytes(colFamily), Bytes.toBytes(colKey),
          Bytes.toBytes(value));
      table.put(p);
      System.out
          .println("================================== done putting values");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    long currentTime = System.currentTimeMillis();
    ProductCountWriter.updateCount("test01-", currentTime, 1);
    ProductCountWriter.updateCount("test01-", currentTime, 1);
    ProductCountWriter.updateCount("test01-", currentTime + 900, 1);

    ProductCountWriter.updateCount("test02-", currentTime, 1);
    ProductCountWriter.updateCount("test02-", currentTime + 70000, 1);
  }

}
