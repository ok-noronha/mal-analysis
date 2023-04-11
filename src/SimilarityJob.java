import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimilarityJob {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String[] tokens = value.toString().split("\t");
      String[] items = tokens[1].split(",");
      int n = items.length;

      for (int i = 0; i < n; i++) {
        String[] a = items[i].split(":");
        String item1 = a[0];
        double rating1;
        try {
          rating1 = Double.parseDouble(a[1]);
        } catch (Exception e) {
          continue;
        }

        for (int j = i + 1; j < n; j++) {
          String[] b = items[j].split(":");
          String item2 = b[0];
          double rating2;
          try {
            rating2 = Double.parseDouble(b[1]);
          } catch (Exception e) {
            continue;
          }
          outKey.set(item1 + "," + item2);
          outValue.set(rating1 + "," + rating2);

          context.write(outKey, outValue);
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

    private DoubleWritable outValue = new DoubleWritable();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      double sum1 = 0.0;
      double sum2 = 0.0;
      double sum3 = 0.0;

      for (Text value : values) {
        String[] ratings = value.toString().split(",");
        double rating1 = Double.parseDouble(ratings[0]);
        double rating2 = Double.parseDouble(ratings[1]);

        sum1 += rating1 * rating2;
        sum2 += rating1 * rating1;
        sum3 += rating2 * rating2;
      }

      double similarity = sum1 / Math.sqrt(sum2 * sum3);
      outValue.set(similarity);

      context.write(key, outValue);
    }
  }

  public static void run() throws Exception {

    Job job = new Job();
    job.setJarByClass(SimilarityJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path("/pp/part-r-00000"));
    DeleteOutputDirectory.deleteIfExists(job, new Path("/sim"));
    FileOutputFormat.setOutputPath(job, new Path("/sim"));
    job.waitForCompletion(true);
    TopNRecommendationsJob.run();
  }
}
