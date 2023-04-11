import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PreprocessJob {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String user;
      String item;
      String rating;
      String[] tokens = value.toString().split(",");
      try {
        user = tokens[0];
        item = tokens[1];
        rating = tokens[5];
      } catch (Exception e) {
        return;
      }

      outKey.set(user);
      outValue.set(item + ":" + rating);

      context.write(outKey, outValue);
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      StringBuilder sb = new StringBuilder();

      for (Text value : values) {
        sb.append(value.toString()).append(",");
      }

      if (sb.length() > 0) {
        sb.setLength(sb.length() - 1);
        outValue.set(sb.toString());
        context.write(key, outValue);
      }
    }
  }

  public static void run() throws Exception {

    Job job = new Job();
    job.setJarByClass(PreprocessJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path("/anilist"));
    DeleteOutputDirectory.deleteIfExists(job, new Path("/pp"));
    FileOutputFormat.setOutputPath(job, new Path("/pp"));
    job.waitForCompletion(true);
    SimilarityJob.run();
  }
}
