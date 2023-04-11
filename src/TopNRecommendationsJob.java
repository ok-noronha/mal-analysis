import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNRecommendationsJob {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String[] tokens = value.toString().split("\t");
      String[] items = tokens[0].split(",");
      double similarity = Double.parseDouble(tokens[1]);

      String item1 = items[0];
      String item2 = items[1];

      outKey.set(item1);
      outValue.set(item2 + "," + similarity);

      context.write(outKey, outValue);
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Double> map = new HashMap<String, Double>();

      for (Text value : values) {
        String[] tokens = value.toString().split(",");
        String item = tokens[0];
        double similarity = Double.parseDouble(tokens[1]);

        map.put(item, similarity);
      }

      List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(map.entrySet());
      Collections.sort(list, new Comparator<Entry<String, Double>>() {
        public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
          return o2.getValue().compareTo(o1.getValue());
        }
      });

      int n = 10; // number of recommendations to generate
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < n && i < list.size(); i++) {
        sb.append(list.get(i).getKey()).append(",");
      }

      outValue.set(sb.toString());
      context.write(key, outValue);
    }
  }

  public static void run() throws Exception {

    Job job = new Job();
    job.setJarByClass(TopNRecommendationsJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path("/sim/part-r-00000"));
    DeleteOutputDirectory.deleteIfExists(job, new Path("/rec"));

    FileOutputFormat.setOutputPath(job, new Path("/rec"));
    job.waitForCompletion(true);
  }
}
