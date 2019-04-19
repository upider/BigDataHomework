import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is main method.
 * 
 * @author guest
 * @version 1.0
 */
public class Hw2Part1 {

  /**
   * This is the Mapper class
   * 
   * @author guest
   * @version 1.0
   */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text newKey = new Text();
    private Text outValue = new Text();

    /**
     * This is the map method.
     * 
     * @author guest
     * @version 1.0
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\n");

      int count = 1;
      while (line.hasMoreTokens()) {
        String tmp = line.nextToken(); // first line string
        StringTokenizer str = new StringTokenizer(tmp); // first line token
        // condition 1
        if (str.countTokens() != 3) {
          continue;
        }

        String source = str.nextToken();
        String destination = str.nextToken();
        // condition 4 time is float
        String test = str.nextToken();
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        if (!(pattern.matcher(test).matches())) {
          continue;
        }
        float time = Float.valueOf(test);

        newKey.set(source + " " + destination);
        outValue.set(Integer.toString(count) + " " + Float.toString(time));
        context.write(newKey, outValue);
      }
    }
  }

  /**
   * This is the class to combine.
   * 
   * @author guest
   * @version 1.0
   */
  public static class FloatAvgCombiner extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    /**
     * This is the reduce method.
     * 
     * @author guest
     * @version 1.0
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;

      for (Text line : values) {
        String tmp = line.toString();
        StringTokenizer str = new StringTokenizer(tmp);
        int c = Integer.valueOf(str.nextToken());
        float avg = Float.valueOf(str.nextToken());
        sum += avg * c;
        count += c;
      }

      result.set(Integer.toString(count) + " " + Float.toString(sum / count));
      context.write(key, result);
    }
  }

  /**
   * This is the Reducer class.
   * 
   * @author guest
   * @version 1.0
   */
  public static class FloatAvgReducer extends Reducer<Text, Text, Text, Text> {

    private Text result_key = new Text();
    private Text result_value = new Text();

    /**
     * This is the reduce method.
     * 
     * @author guest
     * @version 1.0
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;

      for (Text line : values) {
        String tmp = line.toString(); // first line string
        StringTokenizer str = new StringTokenizer(tmp); // first line token
        int c = Integer.valueOf(str.nextToken());
        float avg = Float.valueOf(str.nextToken());
        sum += avg * c;
        count += c;
      }

      // generate result key
      result_key.set(key);

      // generate result value
      double avg_result = (double) (sum / count);
      avg_result = (double) (Math.round(avg_result * 1000) / 1000.0);
      DecimalFormat df = new DecimalFormat("#.000");
      String avg_print = df.format(avg_result);
      result_value.set(Integer.toString(count) + " " + avg_print);

      context.write(result_key, result_value);
    }
  }

  /**
   * This is main method.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "count and avg");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(FloatAvgCombiner.class);
    job.setReducerClass(FloatAvgReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
