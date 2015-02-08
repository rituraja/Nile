package com.insight.nile.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
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
			String order = input.getString(0);
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

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String productId = tuple.getString(0);
			String order = tuple.getString(1);
			String[] orderFields = order.split(",");

			long timestamp =  Long.parseLong(orderFields[0]);
			int count = Integer.parseInt(orderFields[3]); // quantity

			ProductCountWriter.updateCount(productId, timestamp, count);
			collector.emit(new Values(productId, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("productId", "count"));
		}
	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new DummySaleSpout(), 5);

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
