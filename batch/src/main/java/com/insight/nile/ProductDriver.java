package com.insight.nile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProductDriver {

  public static class CategoryMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text outKey = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] col = value.toString().split(",");
      if (col.length < 3) {
        return;
      }

      Set<String> cateSet = uniqueCategories(col[2]);

      for (String category : cateSet) {
        outKey.set(category);
        context.write(outKey, one);
      }
    }

    static Set<String> uniqueCategories(String col) {
      String[] categories = col.split("%");
      Set<String> cateSet = new HashSet<String>(Arrays.asList(categories));
      return cateSet;
    }
  }

  public static class CategoryProductCountReducer extends
      TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable val : values) {
        count += val.get();
      }
      Put put = new Put(Bytes.toBytes(key.toString()));
      put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(count));

      context.write(null, put);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String zookeeperHost = "ip-172-31-3-237.us-west-1.compute.internal";
    conf.set("hbase.zookeeper.quorum", zookeeperHost);

    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: productDriver <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "categorycount");
    job.setJarByClass(ProductDriver.class);
    job.setMapperClass(CategoryMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setCombinerClass(CategoryProductCountReducer.class);
    job.setReducerClass(CategoryProductCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    TableMapReduceUtil.initTableReducerJob("product_count",
        CategoryProductCountReducer.class, job);
//    FileOutputFormat.setOutputPath(job, new Path(
//        otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
