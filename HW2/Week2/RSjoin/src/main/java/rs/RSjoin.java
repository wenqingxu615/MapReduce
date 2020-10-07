package rs;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RSjoin extends Configured implements Tool{
private static final Logger logger = LogManager.getLogger(RSjoin.class);
	static int MAX_Length = 50000;
	public static class Path2Mapper extends Mapper<Object,Text,Text,Text>{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			 
			 String[] nodes = value.toString().split(",");
			 String x = nodes[0];
			 String y = nodes[1];
			 if ((Long.parseLong(x) < MAX_Length) && (Long.parseLong(y) < MAX_Length)) {
			 //edge (x,y) means x follows y on Twitter
			 context.write(new Text(x), new Text("user:" + y));
			 context.write(new Text(y), new Text("follower:" + x));
			 //the output of mapper will have both in and out edge from the same node as key
		}
		}
	}
	
	
			
			
	public static class Path2Reducer extends Reducer<Text, Text,Text,Text>{	
		@Override
		public void reduce(Text key, final Iterable<Text> values, final Context context) 
				throws IOException, InterruptedException{
			// for each node b (key in the reducer) we can join 
			// the 2 edge (a,b) and (b,c)
			// and output the reverse (c,a) as the 3rd edge we hope to find
			// in order to find a triangle
			List<String> u = new ArrayList<String>(); 
			List<String> f = new ArrayList<String>();
			for (final Text value : values) {
				String record = value.toString();
				if (record.startsWith("user")) {
					u.add(record.split(":")[1]);
				}else if(record.startsWith("follower")){
					f.add(record.split(":")[1]);
				}
			}
			for (int i = 0;i < u.size();i++) {
				for (int j = 0; j < f.size(); j++) {
					if (!(u.get(i).equals(f.get(j)))) {
					context.write(new Text(u.get(i)), new Text(f.get(j)));
				}
				}
			}
			}
			
			
		}
	public static class TriangleMapper1 extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			 // This mapper is for the original edges.csv
			String[] nodes = value.toString().split(",");
			 String x = nodes[0];
			 String y = nodes[1];
			 if ((Long.parseLong(x) < MAX_Length) && (Long.parseLong(y) < MAX_Length)) {	 
			 context.write(value, new Text("1"));
			 }
		}
	}
	public static class TriangleMapper2 extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			 // This mapper is for the intermideate result
			 context.write(value, new Text("2"));
		
		}
	}
	
	public static class TriangleReducer extends Reducer<Text, Text,Text,LongWritable>{	
		private LongWritable result = new LongWritable();
		private long v = 0;
		@Override
		public void reduce(Text key, final Iterable<Text> values, final Context context) 
				throws IOException, InterruptedException{
			long edge1 = 0;
			long edge2 = 0;
			for (final Text value : values) {
				String record = value.toString();
				if (record.equals("1")) {
					edge1 += 1;
				}else if(record.equals("2")){
					edge2 += 1;
				}
			}
			v += edge1 * edge2;
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			// divide the answer by 3 because there are duplicates
			v = (long) Math.ceil(v/3);
			result.set(v);
			context.write(new Text("Total"), result);
		
		}
	}

	@Override
	public int run(final String[] args) throws Exception{
		JobControl jobControl = new JobControl("jobChain"); 
	    Configuration conf1 = getConf();
	    conf1.set("mapred.textoutputformat.ignoreseparator","true");
	    conf1.set("mapred.textoutputformat.separator",",");
	    Job job1 = Job.getInstance(conf1);  
	    job1.setJarByClass(RSjoin.class);
	    job1.setJobName("Path2");

	    FileInputFormat.setInputPaths(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

	    job1.setMapperClass(Path2Mapper.class);
	    job1.setReducerClass(Path2Reducer.class);
	    //job1.setCombinerClass(SumReducer.class);

	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    //ControlledJob controlledJob1 = new ControlledJob(conf1);
	    //controlledJob1.setJob(job1);

	    //jobControl.addJob(controlledJob1);
	    int code = (job1.waitForCompletion(true) ? 0 : 1);
	    if (code == 0) { 
	    Configuration conf2 = getConf();

	    Job job2 = Job.getInstance(conf2);
	    job2.setJarByClass(RSjoin.class);
	    job2.setJobName("Triangle");
	    
	    
	    MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class,TriangleMapper1.class);
	    MultipleInputs.addInputPath(job2, new Path(args[1] + "/temp"), TextInputFormat.class,TriangleMapper2.class);
	    //FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));


	    
	    
	    job2.setReducerClass(TriangleReducer.class);
	    //job2.setCombinerClass(SumReducer2.class);

	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    //job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setNumReduceTasks(1);
	    //job2.setSortComparatorClass(IntComparator.class);
	    return(job2.waitForCompletion(true) ? 0 : 1);
	    }
	    //ControlledJob controlledJob2 = new ControlledJob(conf2);
	    //controlledJob2.setJob(job2);

	    // make job2 dependent on job1
	    //controlledJob2.addDependingJob(controlledJob1); 
	    // add the job to the job control
	    //jobControl.addJob(controlledJob2);
	    //Thread jobControlThread = new Thread(jobControl);
	    //jobControlThread.start();
	    /*
	while (!jobControl.allFinished()) {
	    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
	    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
	    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
	    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
	    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
	try {
	    Thread.sleep(5000);
	    } catch (Exception e) {

	    }

	  } 
	   System.exit(0);  
	   */
	   return (job1.waitForCompletion(true) ? 0 : 1);   
	  } 
public static void main(String[] args) throws Exception { 
	if (args.length != 2) {
		throw new Error("Two arguments required:\n<input-dir><output-dir>");
	}
	try {
		ToolRunner.run(new RSjoin(), args);
		
	}catch (final Exception e) {
		logger.error("",e);
	}	
}
}
	