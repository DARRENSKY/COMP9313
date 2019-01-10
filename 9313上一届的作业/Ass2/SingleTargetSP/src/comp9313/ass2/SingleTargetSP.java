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
    
    /*
     * the node class for initiating a node 
     */
    public static class Node {
    	private String distance;		// the distance from source
    	private String[] adjs;			// the neighbors of current node
    	
    	public String getDistance() {
    		return distance;
    	}
    	
    	public void setDistance(String distance) {
    		this.distance = distance;
    	}
    	
    	public void setDistance(int distance) {
    		this.distance = Integer.toString(distance);
    	}
    	
    	public String getKey(String str) {
    		return str.substring(1, str.indexOf(","));
    	}
    	
    	public String getValue(String str) {
    		return str.substring(str.indexOf(",") + 2, str.indexOf(")"));
    	}
    	
    	public String getNodeKeyByIndex(int idx) {
    		return getKey(adjs[idx]);
    	}
    	
    	public String getNodeValueByIndex(int idx) {
    		return getValue(adjs[idx]);
    	}
    	
    	public int getAdjsNum() {
    		return adjs.length;
    	}
    	
    	// formate a node by using a given str
    	public void FormatNode(String str) {
    		// without any neighbors
    		if (str.length() == 0) {
    			return;
    		}
    		// str example
        	// example1 : 1		inf		(3, 1.0)	(2, 3.0)
        	// example1 : 2		5
        	// example1 : 3		inf	
    		String[] strs = StringUtils.split(str, '\t');
    		this.adjs = new String[strs.length - 1];
    		
    		for (int i = 0; i < strs.length; i++) {
    			// the first one is distance
    			if (i == 0) {	
    				setDistance(strs[i]);
    				continue;
    			}
    			// others are all neighbors
    			this.adjs[i - 1] = strs[i];		
    		}
    	}
    	
    	public String toString() {
    		String str = this.distance + "";
    		for(String adj : this.adjs) {
    			str += "\t" + adj;
    		}
    		
    		return str;
    	}    
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

    
    /*
     * STMapper
     */
    public static class STMapper extends Mapper<Object, Text, Text, Text> {
    	/*
    	 * Input: toNodeID distanceFromTargetTotoNode listOfNeighborNodes
    	 * Output: toNodeID distanceFromTargetTotoNode listOfNeighborNodes
    	 *         neighborNodesOfToNodeID distanceFromTargetToNeighborNode
    	 */
    	@Override
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		Node node = new Node();
    		node.FormatNode(value.toString());
    		context.write(new Text(key.toString()), value);
    		// current node is unreachable for source node
    		if (node.getDistance().equals("inf")) {
    			return;
    		}
    		
    		// re-calculate the distance from the target node to
    		// the all neighbors of current node
    		for(int i = 0; i < node.getAdjsNum(); i++) {
    			String k = node.getNodeKeyByIndex(i);
    			String d = node.getNodeValueByIndex(i);
    			String v = new String(Double.parseDouble(d) + Double.parseDouble(node.getDistance()) + "");
    			context.write(new Text(k), new Text(v));
    		}
        }
    }

    /*
     * STReducer
     */
    public static class STReducer extends Reducer<Text, Text, Text, Text> {
    	/*
    	 * Input: toNodeID distanceFromTargetTotoNode listOfNeighborNodes
    	 *        neighborNodesOfToNodeID distanceFromTargetToNeighborNode
    	 * Output: toNodeID shortestDistanceFromTargetTooNode listOfNeighborNodes
    	 */
    	@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        	String minDistance = "inf";
        	String oriDistance = "inf";        	
        	String distance = "inf";
        	String[] tokens = null;
        	Node node = new Node();
        	
        	for (Text value : values) {
        		tokens = StringUtils.split(value.toString(), '\t');
        		// if current node have some neighbors 
        		if (tokens.length > 1) {
        			node.FormatNode(value.toString());
        			oriDistance = tokens[0];
        		}
        		// get the current distance to target node
        		distance = tokens[0];
        		
        		// find the shortest path
        		if (minDistance.equals("inf")) {
        			if (!distance.equals("inf")) {
        				minDistance = distance;   
        			}
        		} else {
        			if (!distance.equals("inf")) {
        				// update if current distance is smaller than minDistance
        				if (Double.parseDouble(distance) < Double.parseDouble(minDistance))
        					minDistance = distance;
        			}
        		}   
        	}

    		if (!oriDistance.equals(minDistance)) {
    			context.getCounter(STP_COUNTER.NODE_UPDATED_COUNT).increment(1);
    		}
        	node.setDistance(minDistance);
        	context.write(key, new Text(node.toString()));
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