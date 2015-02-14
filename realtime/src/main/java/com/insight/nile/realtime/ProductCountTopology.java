package com.insight.nile.realtime;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ProductCountTopology {
  public static class GroupByProduct extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String order;
      try {
        order = new String(input.getBinary(0), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      String[] orderFields = order.split(",");
      collector.emit(new Values(orderFields[2] /* productId */, order));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("productId", "order"));
    }
  }

  public static class ProductCount extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;
    private ProductCountWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      super.prepare(stormConf, context);
      this.writer = new ProductCountWriter();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String productId = tuple.getString(0);
      String order = tuple.getString(1);
      String[] orderFields = order.split(",");

      long timestamp = Long.parseLong(orderFields[0]);
      int count = Integer.parseInt(orderFields[3]); // quantity

      writer.updateCount(productId, timestamp, count);
      collector.emit(new Values(productId, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("productId", "count"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    // builder.setSpout("spout", new DummySaleSpout(), 5);

    // Zookeeper that serves for Kafka queue
    ZkHosts zk = new ZkHosts("ip-172-31-3-237.us-west-1.compute.internal");


    SpoutConfig config = new SpoutConfig(zk, "order", "/order", UUID.randomUUID().toString());

    builder.setSpout("spout", new KafkaSpout(config));

    builder.setBolt("byProductId", new GroupByProduct(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new ProductCount(), 12).fieldsGrouping("byProductId",
        new Fields("productId"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("product-count", conf, builder.createTopology());

      Thread.sleep(10000); // sleep for 10 seconds before shutting down the
                           // local cluster

      cluster.shutdown();
    }
  }
}

