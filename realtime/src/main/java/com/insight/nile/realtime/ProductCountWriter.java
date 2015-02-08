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
  private static final DateTimeFormatter minfmt = ISODateTimeFormat.hourMinute();

  private final HConnection conn;

  /**
   * Creates a writer instance and the underlying connection to HBase.
   */
  public ProductCountWriter() {
    Configuration hBaseConfig = HBaseConfiguration.create();
    String zookeeperHost = "ip-172-31-3-237.us-west-1.compute.internal";
    hBaseConfig.set("hbase.zookeeper.quorum", zookeeperHost);
    try {
      this.conn = HConnectionManager.createConnection(hBaseConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes the underlying HBase connection. This writer instance can't be used
   * after close is called.
   */
  public void close() {
    try {
      conn.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Increments the count for the specified productId in the database.
   */
  public void updateCount(String productId, long timestamp, int count) {
    String day = dayfmt.print(timestamp);
    String rowKey = productId + day;
    String colFamily = "min";
    String colKey = minfmt.print(timestamp);
    int value = count;

    System.out.println(rowKey + " " + colKey + " " + value);
    try {
      HTableInterface table = conn.getTable(TableName.valueOf("productCount_Min"));
      System.out.println("==================================Got connections?");
      Get g = new Get(Bytes.toBytes(rowKey));
      Result row = table.get(g);
      System.out.println("================================== get row");
      if (!row.isEmpty()) {
        System.out.println("================================== row not empty");
        byte[] oldValue = row.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        if (oldValue != null) {
          System.out.println("================================== cell not empty");
          int oldCount = Bytes.toInt(oldValue);
          System.out.println("================================== old count " + oldCount);
          System.out.println("================================== value is " + value);
          value += oldCount;
        }
      }
      System.out.println("=================================new value is " + value);

      Put p = new Put(Bytes.toBytes(rowKey));
      p.add(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(value));
      table.put(p);
      table.close();
      System.out.println("================================== done putting values");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    long currentTime = System.currentTimeMillis();
    ProductCountWriter writer = new ProductCountWriter();
    writer.updateCount("test01-", currentTime, 1);
    writer.updateCount("test01-", currentTime, 1);
    writer.updateCount("test01-", currentTime + 900, 1);

    writer.updateCount("test02-", currentTime, 1);
    writer.updateCount("test02-", currentTime + 70000, 1);
  }

}
