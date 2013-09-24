package edu.fiu.cs.yxjiang.event.stat;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FindMissingMessage extends Configured implements Tool {

  public static class FindMissingMapper extends
      Mapper<LongWritable, Text, LongWritable, IntWritable> {

    private JsonParser parser;
    private IntWritable one;

    @Override
    public void setup(Context context) {
      parser = new JsonParser();
      one = new IntWritable(1);
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      JsonObject messageObj = parser.parse(value.toString()).getAsJsonObject();
      long timestamp = messageObj.get("timestamp").getAsLong();
      context.write(new LongWritable(timestamp), one);
    }
  }

  public static class FindMissingReducer extends
      Reducer<LongWritable, IntWritable, Text, IntWritable> {

    private int correctCount;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      correctCount = Integer.parseInt(conf.get("correctCount"));
    }

    public void reduce(LongWritable key, Iterable<IntWritable> value,
        Context context) throws InterruptedException, IOException {

      Date date = new Date(key.get());
      int count = 0;
      Iterator<IntWritable> iterator = value.iterator();
      while (iterator.hasNext())
        count += iterator.next().get();
      if (count != correctCount)
        context.write(new Text(date.toString()), new IntWritable(count));
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String inputDirPathStr = args[0];
    String outputDirPathStr = args[1];

    Configuration conf = new Configuration();
    conf.setInt("correctCount", 7);
    FileSystem hdfs = FileSystem.get(conf);
    Path inputPath = new Path(inputDirPathStr);
    Path outputPath = new Path(outputDirPathStr);
    if (hdfs.exists(outputPath)) {
      System.out.printf("Output path: %s exists, delete it first.\n",
          outputDirPathStr);
      hdfs.delete(outputPath, true);
    }

    Job job = new Job(conf, "FindMissingMessage");

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setJarByClass(FindMissingMessage.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(FindMissingMapper.class);
    // job.setMapOutputKeyClass(LongWritable.class);
    // job.setMapOutputValueClass(IntWritable.class);

    job.setCombinerClass(FindMissingReducer.class);
    job.setReducerClass(FindMissingReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new FindMissingMessage(), args);
    System.exit(ret);
  }

}
