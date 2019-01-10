package comp9313.ass2;

import java.net.URI;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleTargetSP {
    public static String OUT = "output";
    public static String IN = "input";
    public static String TARGET = null;

    /**
     * Define a pair of integers that are writable.
     * They are serialized in a byte comparable format.
     */
    public static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public void IntPair(int left, int right) {
            set(left, right);
        }
        
        /**
         * Set the left and right values.
         */
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

        /**
         * Read the two integers. 
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }

        @Override
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        /** A Comparator that compares serialized IntPair. */ 
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        }

        @Override
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * Partition based on the first part of the pair.
     */
    public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{
    @Override
    public int getPartition(IntPair key, IntWritable value, 
            int numPartitions) {
        return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    /**
     * Compare only the first part of the pair, so that reduce is called once
     * for each value of the first part.
     */
    public static class FirstGroupingComparator 
        implements RawComparator<IntPair> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
                             b2, s2, Integer.SIZE/8);
        }

        @Override
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }


    public <T> List<T> union(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);
        return new ArrayList<T>(set);
    }

    public <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for (T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }


    /*
     * InitMapper
     */
    public static class InitMapper extends Mapper<Object, Text, Text, Text> {
        /*
        * Input: edgeID fromNodeID toNodeID distance
        * Output: toNodeID fromNodeID distance
        */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), ' ');

            // directed graph
            // swap the "FromNode" with "ToNode"
            // so we can calculate the single target node shortest path
            // to single source node shortest path
            context.write(new Text(tokens[2]), new Text("(" + tokens[1] + ", " + tokens[3] + ")"));
            context.write(new Text(tokens[1]), new Text("(" + tokens[1] + ", " + 0.0 + ")"));
        }
    }
    
    
    /*
     * InitReducer
     */
    public static class InitReducer extends Reducer<Text, Text, Text, Text> {
        /*
        * Input: toNodeID fromNodeID distance
        * Output: toNodeID distanceFromTargetTotoNode listOfNeighborNodes
        */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder adjsPairs = new StringBuilder();
            HashMap<Text, Text> neighbours = new HashMap<Text, Text>();
            // if the node equal to target node
            if (TARGET.equals(key.toString())) {
                adjsPairs.append("0.0");
            } 
            // all nodes are unreachable for target node in the initializing step
            else {
                adjsPairs.append("inf");
            }
            adjsPairs.append('\t');

            for (Text value : values) {
                // check the redundancy of neighbours 
                if (neighbours.containsKey(value)) {
                    continue;
                } else {
                    neighbours.put(value, value);
                }
                adjsPairs.append(value.toString());
                adjsPairs.append('\t');
            }
            context.write(key, new Text(adjsPairs.toString()));
        }
    }


    public static class BKMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        }
    }

    public static class BKReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        }
    }
    
    /*
     * ResultMapper
     */
    public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text> {    	
        /*
         * Input: toNodeID shortestDistanceFromTargetToToNode listOfNeighborNodes
         * Output: toNodeID shortestDistanceFromTargetToToNode
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = StringUtils.split(value.toString(), '\t');
            int nodeID = Integer.parseInt(tokens[0]);
            context.write(new IntWritable(nodeID), new Text(tokens[1]));
        }
    }
    
    
    /*
     * ResultReducer
     */
    public static class ResultReducer extends Reducer<IntWritable, Text, Text, Text> {
        /*
         * Input: toNodeID shortestDistance
         * Output: targetNode toNodeID shortestDistance
         */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // check inf
                // only write the nodes which is reachable for target node
                if (value.toString().equals("inf"))
                    continue;
                context.write(new Text(TARGET), new Text(key.toString() + "\t" + value.toString()));
            }
        }
    }    

    public static enum STP_COUNTER {
        NODE_UPDATED_COUNT;
    }
    
    public static void main(String[] args) throws Exception {        
        IN = args[0];
        OUT = args[1];
        TARGET = args[2];
        
        String input = IN;
        String output = OUT;
        int numOfMapReducer = 3;
        
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            // delete outPath if the path has existed.
            if (fs.exists(new Path(OUT)))
                fs.delete(new Path(OUT), true);   
        } catch (Exception e){} 
        
        // initial stage
        Job job = Job.getInstance(conf, "Init");
        job.setJarByClass(SingleTargetSP.class);
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);        

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);        

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output + "0"));     
        job.waitForCompletion(true);

        int count = 0;
        boolean isdone = false;
        while (isdone == false) {          
            input = OUT + count;
            count += 1;
            output = OUT + count;

            // Dijkstra stage - calculate the shortest path
            Job job1 = Job.getInstance(conf, "Dijkstra");
            job1.setJarByClass(SingleTargetSP.class);
            job1.setMapperClass(STMapper.class);
            job1.setReducerClass(STReducer.class);        

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);   
            
            job1.setNumReduceTasks(numOfMapReducer);
            
            job1.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job1, new Path(input));
            FileOutputFormat.setOutputPath(job1, new Path(output)); 
            job1.waitForCompletion(true);
            
            // get counter from JOB
            Counters counters = job1.getCounters();
            Counter counter = counters.findCounter(STP_COUNTER.NODE_UPDATED_COUNT);
            
            if(counter.getValue() == 0) {
                isdone = true;
            }
            
            try {
                FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                // remove input folder
                if (fs.exists(new Path(input)))
                    fs.delete(new Path(input), true);
            } catch (Exception e) {}

        }

        // final stage - print the result according to the require
        conf.set("mapred.textoutputformat.separator", "\t");
        Job job2 = Job.getInstance(conf, "Result");
        job2.setJarByClass(SingleTargetSP.class);
        job2.setMapperClass(ResultMapper.class);
        job2.setReducerClass(ResultReducer.class);        

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);        

        FileInputFormat.addInputPath(job2, new Path(output));
        FileOutputFormat.setOutputPath(job2, new Path(OUT));     
        job2.waitForCompletion(true);
        
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            if (fs.exists(new Path(output)))
                fs.delete(new Path(output), true);
        } catch (Exception e) {}

        System.exit(0);
    }
}