package comp9313.ass4;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.lang.Math;


import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class SetSimJoin {
    public static String OUT = "output";
    public static String IN = "input";
    public static String SortedToken = "";
    public static double SIM = 0.5;
    public static int NumOfCluster = 1;

    /**
     * Define a pair of integers that are writable.
     * They are serialized in a byte comparable format.
     */
    public static class IntPair implements WritableComparable<IntPair> {
        int first, second;

//        public IntPair() {}
//        
//        public IntPair(int left, int right) {
//        	set(left, right);
//        }
        
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }
        
        public String toString(){
            return "(" + first + "," + second + ")";
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            // TODO Auto-generated method stub
            first = arg0.readInt();
            second = arg0.readInt();
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            // TODO Auto-generated method stub
            arg0.writeInt(first);
            arg0.writeInt(second);
        }

        @Override
        public int compareTo(IntPair arg0) {
            // TODO Auto-generated method stub
            if (first != arg0.first) {
                return first < arg0.first ? -1 : 1;
            } else if (second != arg0.second) {
                return second < arg0.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        public int hashCode() {
            return first * 157 + second;
        }

        public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
    }

    public static class FirstPartitioner extends
        Partitioner<IntPair, IntWritable> { 

        @Override
        public int getPartition(IntPair arg0, IntWritable arg1, int arg2) {
            // TODO Auto-generated method stub
            return Math.abs((arg0.getFirst() * 127) % arg2);
        }
    }

    /*
     * Method 1
     * 
     * public static class GroupingComparator implements RawComparator<IntPair>{
     * 
     * @Override public int compare(IntPair arg0, IntPair arg1) { // TODO
     * Auto-generated method stub int l = arg0.getFirst(); int r =
     * arg0.getFirst(); return l == r ? 0 : (l < r ? -1 : 1); }
     * 
     * @Override public int compare(byte[] b1, int s1, int l1, byte[] b2, int
     * s2, int l2) { return WritableComparator.compareBytes(b1, s1,
     * Integer.SIZE/8, b2, s2, Integer.SIZE/8); } }
     */

    // Method 2
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(IntPair.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair i1 = (IntPair) w1;
            IntPair i2 = (IntPair) w2;
            int l = i1.getFirst();
            int r = i2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    public static <T> List<T> union(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);
        return new ArrayList<T>(set);
    }

    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for (T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }    
    
    /*
     * Stage 1 - Phase 1 - Basic Token Ordering Mapper
     */
    public static class BTOS1Mapper extends Mapper<Object, Text, Text, IntWritable> {
    	IntWritable one = new IntWritable(1);
    	Text token = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	StringTokenizer itr = new StringTokenizer(value.toString(), " ");
        	if(itr.countTokens() > 1) {
        		itr.nextToken();
        		while(itr.hasMoreTokens()) {
        			token.set(itr.nextToken());
        			context.write(token, one);
        		}
        	}
        }
    }

    /*
     * Stage 1 - Phase 1 - Basic Token Ordering Reducer - Count token frequency
     */
    public static class BTOS1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	IntWritable totalCount = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
        	int sum = 0;
        	for(IntWritable value : values) {
        		sum += Integer.parseInt(value.toString());
        	}
        	totalCount.set(sum);
        	context.write(key, totalCount);
        }
    }
    
    
    public static class BTOS2Partitioner extends Partitioner<IntWritable, Text>{
		@Override
		public int getPartition(IntWritable arg0, Text arg1, int arg2) {
			// TODO Auto-generated method stub
			return Math.abs((arg0.hashCode() * 127) % arg2);
		}
    	
    }

    /*
     * Stage 1 - Phase 2 - Basic Token Ordering - output the token according to the token frequency
     */
    public static class BTOS2Reducer extends Reducer<IntWritable, Text, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        	List<Integer> tokens = new ArrayList<Integer>();
        	for(Text value : values) {
        		tokens.add(Integer.parseInt(value.toString()));
        	}
        	Collections.sort(tokens);
        	for (Integer value : tokens) {
        		context.write(new IntWritable(value), NullWritable.get());
        	}
        }
    }    
    
    /*
     * Stage 2 - Basic Kernel 
     */
    public static class BKMapper extends Mapper<Object, Text, Text, Text> {
    	Map<String, Integer> globalFreqMap = new HashMap<String, Integer>();
  
    	/*
    	 * Read the token - global frequency information for ordering local file
    	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
    	 */
    	@Override
    	public void setup(Context context) throws IOException, InterruptedException {
    		String globleFreqFile = SortedToken + "/part-r-00000";
    		FileSystem fs = null;
    		try {
    			Configuration conf = new Configuration();
    			fs = FileSystem.get(new URI(globleFreqFile), conf);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(globleFreqFile))));
    		String line;
    		int count = 1;
    		while((line = br.readLine()) != null){
    			globalFreqMap.put(line, count++);
    		}
    	}
    	
    	/*
    	 * read the local file and order that according to the global token frequency
    	 * output first p token with the whole file
    	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
    	 */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Map<Integer, String> localFreqMap = new HashMap<Integer, String>();
        	StringTokenizer itr = new StringTokenizer(value.toString(), " ");
        	if(itr.countTokens() > 1) {
        		itr.nextToken();
        		while(itr.hasMoreTokens()) {
        			String token = itr.nextToken();
        			int order = globalFreqMap.get(token);
        			localFreqMap.put(order, token);
        		}
        	}
        	
        	int len = localFreqMap.size();
        	int pLen = len + 1 - (int)Math.ceil(len * SIM);
        	Map<Integer, String> sortedLocalTokens = new TreeMap<Integer, String>(localFreqMap);

        	for(Map.Entry<Integer, String> token : sortedLocalTokens.entrySet()) {
        		if(pLen <= 0) {
        			break;
        		}
        		context.write(new Text(token.getValue()), new Text(value.toString()));
        		pLen--;
        	}
//        	assume that the input file is sorted, so we don't need to sort that
//        	if(itr.countTokens() > 1) {
//        		itr.nextToken();
//        		int len = itr.countTokens();
//        		int pLen = len + 1 - (int)Math.ceil(len * SIM);
////        		System.out.println("rid = " + RID);
////        		System.out.println("len = " + len);
////        		System.out.println("sim = " + SIM);
////        		System.out.println("pre = " + pLen + "\n");
//        		for(int i = 0; i < pLen; ++i) {
//        			// value should be (string-int) pairs 
//        			// we can use innner-mapper in here to improce peformance!!!
//        			context.write(new Text(itr.nextToken()), new Text(value.toString()));
//        		}
//        	}
        }
    }  
    
    /*
     * BK partitioner, all the same key will send to the same reducer
     */
	public static class BKPartitioner extends Partitioner<Text, Text> {
	
		@Override
	    public int getPartition(Text arg0, Text arg1, int arg2) {
	        // TODO Auto-generated method stub
	        return Math.abs((arg0.hashCode() * 127) % arg2);
	    }
	}
    
    public static class BKReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        	List<String> doc = new ArrayList<String>();
        	for (Text value : values) {
        		// do not compare to self
        		for(String d : doc){
        			// calculate the similarity in there and emit that
        			List<String> str1 = new ArrayList<String>(Arrays.asList(d.split(" ")));
        			List<String> str2 = new ArrayList<String>(Arrays.asList(value.toString().split(" ")));
        			int RID1 = Integer.parseInt(str1.remove(0));
        			int RID2 = Integer.parseInt(str2.remove(0));
        			List<String> IntersectionList = intersection(str1, str2);
        			List<String> UnionList = union(str1, str2);
        			double similarity = (double) IntersectionList.size() / UnionList.size();
        			
        			if(similarity >= SIM) {
        				if(RID1 < RID2) {
        					context.write(new Text(RID1 + "\t" + RID2), new DoubleWritable(similarity));
        				} else if (RID1 > RID2) {
        					context.write(new Text(RID2 + "\t" + RID1), new DoubleWritable(similarity));
        				}
        			}
        		}
        		doc.add(value.toString());
        	}
        }
    }    
    
    /*
     * Stage 3 - Remove Duplicate Records
     */
    public static class RDPMapper extends Mapper<Object, Text, IntPair, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	// format: RID1 \t RID2 \t SIM
        	IntPair intPairs = new IntPair();
        	StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        	if(itr.countTokens() == 3) {
        		int RID1 = Integer.parseInt(itr.nextToken());
        		int RID2 = Integer.parseInt(itr.nextToken());
        		double similarity = Double.parseDouble(itr.nextToken());
        		intPairs.set(RID1, RID2);
        		context.write(intPairs, new DoubleWritable(similarity));
        	}
        	
        }
    } 
    
    /*
     * Stage 3 - Mapper Combiner
     */
    public static class RDPCombiner extends Reducer<IntPair, DoubleWritable, IntPair, DoubleWritable> {
        @Override
        public void reduce(IntPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException { 
        	for (DoubleWritable value : values) {
        		context.write(key, value);
        		break;
        	}
        }
    } 
    
    /*
     * Stage 3 - Write the final result to hdfs, one key only write one time.
     */
    public static class RDPReducer extends Reducer<IntPair, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(IntPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException { 
        	for (DoubleWritable value : values) {
        		context.write(new Text(key.toString()), value);
        		break;
        	}
        }
    }      
    
    public static void main(String[] args) throws Exception {        
        IN = args[0];
        OUT = args[1];
        SIM = Double.parseDouble(args[2]);
        NumOfCluster = Integer.parseInt(args[3]);
        SortedToken = OUT + "_SortedToken";
        String CountedToken = OUT + "_CountedToken";
        String unsortedResult = OUT + "_UnsortedResult";
        
        Configuration conf = new Configuration();
//        FileSystem.get(conf).delete(new Path(CountedToken));
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            // delete outPath if the path has existed.
            if (fs.exists(new Path(OUT)))
                fs.delete(new Path(OUT), true); 
            if (fs.exists(new Path(SortedToken)))
                fs.delete(new Path(SortedToken), true); 
            if (fs.exists(new Path(CountedToken)))
                fs.delete(new Path(CountedToken), true); 
            if (fs.exists(new Path(unsortedResult)))
                fs.delete(new Path(unsortedResult), true); 
        } catch (Exception e){} 
        
        // BTO - Phase 1
        Job jobBTOS1 = Job.getInstance(conf, "BTOS1");
        jobBTOS1.setJarByClass(SetSimJoin.class);
        jobBTOS1.setMapperClass(BTOS1Mapper.class);
        jobBTOS1.setReducerClass(BTOS1Reducer.class); 
        jobBTOS1.setCombinerClass(BTOS1Reducer.class); 

        jobBTOS1.setMapOutputKeyClass(Text.class);
        jobBTOS1.setMapOutputValueClass(IntWritable.class);
        jobBTOS1.setOutputKeyClass(Text.class);
        jobBTOS1.setOutputValueClass(IntWritable.class);   
        jobBTOS1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(jobBTOS1, new Path(IN));
        FileOutputFormat.setOutputPath(jobBTOS1, new Path(CountedToken));     
        jobBTOS1.waitForCompletion(true);

//        System.exit(0);
      
        // BTO - Phase 2
        Job jobBTOS2 = Job.getInstance(conf, "BTOS2");
        jobBTOS2.setJarByClass(SetSimJoin.class);
        jobBTOS2.setMapperClass(InverseMapper.class);
        jobBTOS2.setReducerClass(BTOS2Reducer.class);  
        jobBTOS2.setPartitionerClass(BTOS2Partitioner.class);
        jobBTOS2.setInputFormatClass(SequenceFileInputFormat.class);

        jobBTOS2.setMapOutputKeyClass(IntWritable.class);
        jobBTOS2.setMapOutputValueClass(Text.class);
        jobBTOS2.setOutputKeyClass(IntWritable.class);
        jobBTOS2.setOutputValueClass(NullWritable.class);   
        
        FileInputFormat.addInputPath(jobBTOS2, new Path(CountedToken));
        FileOutputFormat.setOutputPath(jobBTOS2, new Path(SortedToken));     
        jobBTOS2.waitForCompletion(true);
        
//        System.exit(0);
        
        // BK stage
        Job jobBK = Job.getInstance(conf, "BK");
        jobBK.setJarByClass(SetSimJoin.class);
        jobBK.setMapperClass(BKMapper.class);
        jobBK.setReducerClass(BKReducer.class);  
        jobBK.setPartitionerClass(BKPartitioner.class);
        jobBK.setNumReduceTasks(NumOfCluster);

        jobBK.setMapOutputKeyClass(Text.class);
        jobBK.setMapOutputValueClass(Text.class);
        jobBK.setOutputKeyClass(Text.class);
        jobBK.setOutputValueClass(DoubleWritable.class);        

        FileInputFormat.addInputPath(jobBK, new Path(IN));
        FileOutputFormat.setOutputPath(jobBK, new Path(unsortedResult));     
        jobBK.waitForCompletion(true);
//        System.exit(0);
        
        // RDP stage
        Job jobRDP = Job.getInstance(conf, "RDP");
        jobRDP.setJarByClass(SetSimJoin.class);
        jobRDP.setMapperClass(RDPMapper.class);
        jobRDP.setCombinerClass(RDPCombiner.class);
        jobRDP.setReducerClass(RDPReducer.class);       
        
        // group and partition by the first int in the pair
//        jobRDP.setPartitionerClass(FirstPartitioner.class);
//        jobRDP.setGroupingComparatorClass(GroupingComparator.class);
//        jobRDP.setPartitionerClass(RDPPartitioner.class);
//        jobRDP.setSortComparatorClass(KeyComparator.class);
        jobRDP.setMapOutputKeyClass(IntPair.class);
        jobRDP.setMapOutputValueClass(DoubleWritable.class);
        jobRDP.setOutputKeyClass(Text.class);
        jobRDP.setOutputValueClass(DoubleWritable.class);        

        FileInputFormat.addInputPath(jobRDP, new Path(unsortedResult));
        FileOutputFormat.setOutputPath(jobRDP, new Path(OUT));     
        jobRDP.waitForCompletion(true);

        System.exit(0);
    }
}
