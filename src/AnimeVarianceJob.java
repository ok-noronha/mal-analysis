import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnimeVarianceJob {

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outKey = new Text();
    private DoubleWritable outValue = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String animeId;
      double score;

      String[] tokens = value.toString().split(",");
      try {
        animeId = tokens[1];
        score = Double.parseDouble(tokens[5]);
      } catch (Exception e) {
        return;
      }

      outKey.set(animeId);
      outValue.set(score);

      context.write(outKey, outValue);
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable outValue = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {

      List<Double> list = new ArrayList<Double>();

      for (DoubleWritable value : values) {
        list.add(value.get());
      }

      double sum = 0.0;
      for (Double d : list) {
        sum += d;
      }
      double mean = sum / list.size();

      double variance = 0.0;
      for (Double d : list) {
        variance += Math.pow(d - mean, 2);
      }
      variance /= list.size();

      outValue.set(variance);
      if (variance > 15) {
        context.write(key, outValue);
      }
    }
  }

  public static void run() throws Exception {

    Job job = new Job();
    job.setJarByClass(AnimeVarianceJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path("/anilist"));
    DeleteOutputDirectory.deleteIfExists(job, new Path("/ur"));
    FileOutputFormat.setOutputPath(job, new Path("/ur"));
    job.waitForCompletion(true);
    NameJob.run();
  }
}
