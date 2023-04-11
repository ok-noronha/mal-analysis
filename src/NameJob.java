import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NameJob {

  public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String animeId;
      String score;

      String[] tokens = value.toString().split("\t");
      try {
        animeId = tokens[0];
        score = "var!:!" + tokens[1];
      } catch (Exception e) {
        return;
      }

      outKey.set(animeId);
      outValue.set(score);

      context.write(outKey, outValue);
    }
  }

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String animeId;
      String score;

      String[] tokens = value.toString().split(",");
      try {
        animeId = tokens[0];
        score = "nam!:!" + tokens[1];
      } catch (Exception e) {
        return;
      }

      outKey.set(animeId);
      outValue.set(score);

      context.write(outKey, outValue);
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      String var = "";
      String nam = "";
      for (Text value : values) {
        String[] f = value.toString().split("!:!");
        if (f[0].equals("var")) {
          var = f[1];
        } else {
          nam = f[1];
        }
      }
      if (!(nam.isEmpty() || var.isEmpty())) {
        context.write(new Text(nam), new Text(var));
      }
    }
  }

  public static void run() throws Exception {

    Job job = new Job();
    job.setJarByClass(AnimeVarianceJob.class);
    // job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path("/ur/part-r-00000"), TextInputFormat.class, Map1.class);
    MultipleInputs.addInputPath(job, new Path("/anis"), TextInputFormat.class, Map2.class);
    DeleteOutputDirectory.deleteIfExists(job, new Path("/under"));
    FileOutputFormat.setOutputPath(job, new Path("/under"));
    job.waitForCompletion(true);
  }
}
