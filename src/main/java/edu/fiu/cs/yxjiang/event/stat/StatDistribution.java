package edu.fiu.cs.yxjiang.event.stat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import edu.fiu.cs.yxjiang.event.util.MessageRetriever;

public class StatDistribution {

  public static class StatDistributionMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    private JsonParser jsonParser;
    private String retrievePath;

    @Override
    public void setup(Context context) {
      jsonParser = new JsonParser();
      retrievePath = context.getConfiguration().get("retrieve.path");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        JsonElement elem = jsonParser.parse(value.toString());
        String ip = MessageRetriever.get("machineIP", elem).getAsString();
        double cpuUtil = MessageRetriever.get(retrievePath, elem).getAsDouble();
        cpuUtil = (double)Math.round(cpuUtil * 1000) / 1000;
        context.write(new Text(ip), new Text(String.format("%f.3", cpuUtil)));
      } catch (Exception e) {
      }
    }
  }

  public static class StatDistributionReducer extends
      Reducer<Text, Text, Text, Text> {

    private TreeMap<String, Long> histogram;

    public void setup(Context context) {
      histogram = new TreeMap<String, Long>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Iterator<Text> itr = values.iterator();
      while (itr.hasNext()) {
        String val = itr.next().toString();
        Long count = histogram.get(val);
        if (count == null) {
          count = new Long(1);
        }
        histogram.put(val, count + 1);
      }
      StringBuilder sb = new StringBuilder();
      int i = 0;
      for (Map.Entry<String, Long> entry : histogram.entrySet()) {
        sb.append(entry.getKey());
        sb.append(":");
        sb.append(entry.getValue());
        if (++i < histogram.size() - 1) {
          sb.append("-");
        }
      }
      context.write(key, new Text(sb.toString()));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        "io.compression.codecs",
        "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");
    conf.set("mapred.job.tracker", "bigdata-node01.cs.fiu.edu:30001");
    // conf.set("dfs.default.name", "datamining-node01.cs.fiu.edu:30002");

    conf.set("retrieve.path", "cpu.idleTime");

    StringBuilder sb = new StringBuilder();
    sb.append("/home/users/yjian004/event/logs");
    
//    String[] inputPaths = {
//        "/home/users/yjian004/event/logs/data.log.2013-03-31",
////        "/user/yjian004/event/logs/data.log.2013-04-01",
////        "/user/yjian004/event/logs/data.log.2013-04-02",
////        "/user/yjian004/event/logs/data.log.2013-04-03",
////        "/user/yjian004/event/logs/data.log.2013-04-04",
////        "/user/yjian004/event/logs/data.log.2013-04-05",
////        "/user/yjian004/event/logs/data.log.2013-04-06",
////        "/user/yjian004/event/logs/data.log.2013-04-07",
////        "/user/yjian004/event/logs/data.log.2013-04-08",
////        "/user/yjian004/event/logs/data.log.2013-04-09",
////        "/user/yjian004/event/logs/data.log.2013-04-10",
//        };
//
//    for (int i = 0; i < inputPaths.length; ++i) {
//      sb.append(inputPaths[i]);
//      if (i != inputPaths.length - 1) {
//        sb.append(',');
//      }
//    }

    String outputDirPathStr = "/home/users/yjian004/event/statDistribution";
    // Path inputDirPath = new Path(inputDirPathStr);
    Path outputDirPath = new Path(outputDirPathStr);

    FileSystem hdfs = FileSystem.get(conf);
    System.out.println(hdfs.getClass().getName());
    if (hdfs.exists(outputDirPath)) {
      hdfs.delete(outputDirPath, true);
      System.out.printf("Output path: %s exists, delete it first.\n",
          outputDirPathStr);
    }

    Job job = new Job(conf, "StatDistribution");
    job.setJarByClass(StatDistribution.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(StatDistributionMapper.class);
    // job.setCombinerClass(StatDistributionReducer.class);
    job.setReducerClass(StatDistributionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPaths(job, sb.toString());
    FileOutputFormat.setOutputPath(job, outputDirPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
