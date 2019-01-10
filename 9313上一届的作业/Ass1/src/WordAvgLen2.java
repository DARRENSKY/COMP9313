package comp9313.ass1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAvgLen2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text firstCharacter = new Text();
    private IntWritable length = new IntWritable();
    private String word;
    private String reg = "[a-zA-Z]";

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  
      StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_0123456789");
      while (itr.hasMoreTokens()) {
    	word = itr.nextToken().toLowerCase();
        if (word.substring(0, 1).matches(reg)) {
        	firstCharacter.set(word.substring(0, 1));
        	length.set(word.length());
        	context.write(firstCharacter, length);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable avgLength = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sumLength = 0;
      int count = 0;
      for (IntWritable val : values) {
    	  sumLength += val.get();
    	  count += 1;
      }
      avgLength.set((double)sumLength / count);
      context.write(key, avgLength);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordAvgLen2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}