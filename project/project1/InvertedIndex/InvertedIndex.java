//package project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {

    private static enum UpdateCount{
        CNT
    }
    public static class LineCntMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text keyEmit = new Text("Total Lines");
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context){
            try {
                context.write(keyEmit, one);
            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(0);
            }
            context.getCounter(UpdateCount.CNT).increment(1);
        }

    }

    public static class LineCntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context){
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
//            String N =context.getConfiguration().get("Sum");
//            System.out.println(Integer.valueOf(N));
            try {
                context.write(key, new IntWritable(sum));

            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }






    public static class InvertedIndexMapper extends Mapper<Object, Text, StringPair, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private StringPair wordPair = new StringPair();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

            ArrayList<String> termArray = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                termArray.add(itr.nextToken().toLowerCase());
            }



            String doc_id = termArray.get(0);

            Set<String> hs = new HashSet<String>();

            for(int i=1;i<termArray.size();i++){
                String term1 = termArray.get(i);

                //This is for nonsymmetric computation
                wordPair.set(term1, doc_id);
                context.write(wordPair, one);

                boolean contains = hs.contains(term1 + " *");

                if (contains){
                }else {
                    hs.add(term1 + " *");
                    wordPair.set(term1, "*");
                    context.write(wordPair, one);


                }
            }
        }
    }






    public static class InvertedIndexCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class InvertedIndexPartitioner extends Partitioner<StringPair, IntWritable>{

        public int getPartition(StringPair key, IntWritable value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

    }











    //The order is decided by the compareTo() function of StringPair
    public static class InvertedIndexReducer extends Reducer<StringPair, IntWritable, Text, Text> {
        private Text finalkey = new Text();
        private DoubleWritable result = new DoubleWritable();
        private Text finalresult = new Text();

        private double n = 0;

        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String Nstring =context.getConfiguration().get("Sum");
            int N= Integer.valueOf(Nstring);
//            int N = 10000;

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }


            String second = key.getSecond();

            if(!second.equals("*")){
                double weight = sum * Math.log10((N/n));
                result.set(weight);


                String finalterm1 = key.getFirst();
                String finalterm2 = key.getSecond();
                finalkey.set(finalterm1 + "\t");
                finalresult.set(finalterm2 + "," + result);

                context.write(finalkey, finalresult);


            }else{
                //for a special key, we only record its value, for further computation
                n = sum;
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1,"line counter");
        j1.setJarByClass(InvertedIndex.class);
        j1.setMapperClass(LineCntMapper.class);
        j1.setReducerClass(LineCntReducer.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(LongWritable.class);
        j1.setOutputValueClass(Text.class);
        Path outputPath=new Path("FirstMapper");
        FileInputFormat.addInputPath(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1,outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath);
        j1.waitForCompletion(true);
        long c = j1.getCounters().findCounter(UpdateCount.CNT).getValue();

        String lingcounter=String.valueOf(c);

        Configuration conf2=new Configuration();
        conf2.set("Sum",lingcounter);
//        System.out.print(String.valueOf(lingcounter));
        Job j2=Job.getInstance(conf2,"word count");
        j2.setJarByClass(InvertedIndex.class);
        j2.setMapperClass(InvertedIndexMapper.class);
        j2.setCombinerClass(InvertedIndexCombiner.class);
        j2.setReducerClass(InvertedIndexReducer.class);
        j2.setPartitionerClass(InvertedIndexPartitioner.class);
        j2.setMapOutputKeyClass(StringPair.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
//        j2.setNumReduceTasks(2);
//        j2.setNumReduceTasks(1);
//        j2.setOutputKeyClass(Text.class);
//        j2.setOutputValueClass(IntWritable.class);
        j2.setNumReduceTasks(new Integer(args[3]));
        Path outputPath1=new Path(args[1]);
        FileInputFormat.addInputPath(j2, new Path(args[0]));
        FileOutputFormat.setOutputPath(j2, outputPath1);
        outputPath1.getFileSystem(conf2).delete(outputPath1, true);
        System.exit(j2.waitForCompletion(true)?0:1);

        /*
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nonsymmetric relative frequency v2");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //either add this partitioner, or override the hashCode() function in StringPair
        job.setPartitionerClass(InvertedIndexPartitioner.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
//        job.setNumReduceTasks(new Integerargs[3);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        */
    }
}

