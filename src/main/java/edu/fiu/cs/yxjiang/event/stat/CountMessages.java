package edu.fiu.cs.yxjiang.event.stat;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CountMessages {

  public static class CountMessageMapper extends
      Mapper<LongWritable, Text, Text, LongWritable> {

    private JsonParser jsonParser;
    private LongWritable one;

    @Override
    public void setup(Context context) {
      jsonParser = new JsonParser();
      one = new LongWritable(1);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        JsonElement jsonElem = jsonParser.parse(value.toString());
        JsonObject jsonObj = jsonElem.getAsJsonObject();
        String machineIP = jsonObj.get("machineIP").getAsString();
        context.write(new Text(machineIP), one);
      } catch (Exception e) {
//        System.err.printf("Malformed Json:\n %s\n", value.toString());
//        context.write(new Text("Malformed"), one);
      }         
    }
  }

  public static class CountMessageReducer extends
      Reducer<Text, LongWritable, Text, LongWritable> {
    
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long count = 0;
      for (LongWritable value : values)
        count += value.get();
      context.write(key, new LongWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec");
    conf.set("mapred.job.tracker", "datamining-node01.cs.fiu.edu:30001");
//    conf.set("dfs.default.name", "datamining-node01.cs.fiu.edu:30002");
    
    
    StringBuilder sb = new StringBuilder();
//    sb.append("/user/yjian004/event/logs");
    String[] inputPaths = {
      "/user/yjian004/event/logs/data.log.2013-03-31",
      "/user/yjian004/event/logs/data.log.2013-04-01",
      "/user/yjian004/event/logs/data.log.2013-04-02",
//      "/user/yjian004/event/logs/data.log.2013-04-03",
//      "/user/yjian004/event/logs/data.log.2013-04-04"
//      "/user/yjian004/event/logs/data.log.2013-04-05",
//      "/user/yjian004/event/logs/data.log.2013-04-06",
//      "/user/yjian004/event/logs/data.log.2013-04-07",
//      "/user/yjian004/event/logs/data.log.2013-04-08",
//      "/user/yjian004/event/logs/data.log.2013-04-09",
//      "/user/yjian004/event/logs/data.log.2013-04-10",
    };
    
    for (int i = 0; i < inputPaths.length; ++i) {
      sb.append(inputPaths[i]);
      if (i != inputPaths.length - 1) {
        sb.append(',');
      }
    }
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

//    if (otherArgs.length != 2) {
//      System.err.println("usage: countMessages inputDirPath outputDirPath");
//      System.exit(1);
//    }

//    String inputDirPathStr = otherArgs[0];
//    String outputDirPathStr = otherArgs[1];
    
    String outputDirPathStr = "/user/yjian004/event/statResult";
//    Path inputDirPath = new Path(inputDirPathStr);
    Path outputDirPath = new Path(outputDirPathStr);
    
    FileSystem hdfs = FileSystem.get(conf);
    System.out.println(hdfs.getClass().getName());
    if (hdfs.exists(outputDirPath)) {
      hdfs.delete(outputDirPath, true);
      System.out.printf("Output path: %s exists, delete it first.\n", outputDirPathStr);
    }

    Job job = new Job(conf, "CountMessage");
    job.setJarByClass(CountMessages.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(CountMessageMapper.class);
    job.setCombinerClass(CountMessageReducer.class);
    job.setReducerClass(CountMessageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPaths(job, sb.toString());
    FileOutputFormat.setOutputPath(job, outputDirPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}







