package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordAvgLen1 {

	public static class IntPair implements WritableComparable<IntPair> {
		private int first;
		private int second;
		
		public IntPair() {}
	  
		public IntPair(int firstNum, int secondNum) {
			set(firstNum, secondNum);
		}
	  
		public void set(int firstNum, int secondNum) {
			first = firstNum;
			second = secondNum;
		}
	  
		public int getFirst() {
			return first;
		}
	  
		public int getSecond() {
			return second;
		}
	  
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}
	  
		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readInt();
		}
	  
		@Override
		public int hashCode() {
			return first * 163  + second;
		}
	  
		@Override
		public boolean equals(Object o) {
			if (o instanceof IntPair) {
				IntPair intPair = (IntPair) o;
				return  (first == intPair.first) && (second == intPair.second);
			}
			return false;
		}
	  
		@Override
		public int compareTo(IntPair o) {
			if (first < o.first) {
				return first < o.first ? -1 : 1;
			} else if (second < o.second) {
				return second < o.second ? -1 : 1;
			} else {
				return 0;
			}
		}
		
		public static class Comparator extends WritableComparator {
			public Comparator() {
				super(IntPair.class);
			}
		  
			public int compare(byte[] b1, int s1, int l1,
							   byte[] b2, int s2, int l2) {
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}
			static {
				WritableComparator.define(IntPair.class, new Comparator());
			}
		}
	} 	// end of IntPair

	
	public static class TokenizerMapper
		extends Mapper<Object, Text, Text, IntPair>{

		private Text firstCharacter = new Text();
		IntPair intPair = new IntPair();

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
					intPair.set(word.length(), 1);
					context.write(firstCharacter, intPair);
				}
			}
		}
	}
	
	public static class IntPairPartialSumCombiner
		extends Reducer<Text,IntPair,Text,IntPair> {
		private IntPair partialSum = new IntPair();

		public void reduce(Text key, Iterable<IntPair> values,
							Context context
				) throws IOException, InterruptedException {
			int sumLen = 0;
			int count = 0;
			for (IntPair val : values) {
				sumLen += val.getFirst();
				count += val.getSecond();
			}
			partialSum.set(sumLen, count);
			context.write(key, partialSum);
		}
	}	

	public static class IntPairReducer
       	extends Reducer<Text,IntPair,Text,DoubleWritable> {
		private DoubleWritable avgLen = new DoubleWritable();

		public void reduce(Text key, Iterable<IntPair> values,
				Context context
				) throws IOException, InterruptedException {
			System.out.println("TEST ");
			int sumLen = 0;
			int count = 0;
			for (IntPair val : values) {
				sumLen += val.getFirst();
				count += val.getSecond();
			}
			avgLen.set((double)sumLen / count);
			context.write(key, avgLen);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordAveLength.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntPairPartialSumCombiner.class);
		job.setReducerClass(IntPairReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}