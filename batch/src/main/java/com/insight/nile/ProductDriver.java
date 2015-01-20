package com.insight.nile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProductDriver {

  public static class CategoryMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text outKey = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] col = value.toString().split("\t");
      if (col.length < 2) {
        return;
      }

      Set<String> cateSet = uniqueCategories(col[2]);

      for (String category : cateSet) {
        outKey.set(category);
        context.write(outKey, one);
      }
    }

    static Set<String> uniqueCategories(String col) {
      String[] categories = col.split("\\|");
      Set<String> cateSet = new HashSet<>(Arrays.asList(categories));
      return cateSet;
    }
  }

  public static class CategoryProductCountReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable val : values) {
        count += val.get();
      }
      result.set(count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: productDriver <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "categorycount");
    job.setJarByClass(ProductDriver.class);
    job.setMapperClass(CategoryMapper.class);
    job.setCombinerClass(CategoryProductCountReducer.class);
    job.setReducerClass(CategoryProductCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job, new Path(
        otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
