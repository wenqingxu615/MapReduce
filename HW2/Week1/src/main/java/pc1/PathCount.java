package pc1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PathCount extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(PathCount.class);
	
	public static class CountMapper extends Mapper<Object,Text,Text,Text>{
		private Map<String,Integer> in;
		private Map<String,Integer> out;
		public void setup(Context context) throws IOException, InterruptedException{
			 in = new HashMap<String,Integer>();
			 out = new HashMap<String,Integer>();
			
		}
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			 
			 String[] nodes = value.toString().split(",");
			 String in_id = nodes[0];
			 String out_id = nodes[1];
			
			//if ((in_id.length() < 7) && (out_id.length() < 7 )) {
			if (in.containsKey(in_id)){
				int totalin = in.get(in_id) + 1;
				in.put(in_id,totalin);
			}else {
				in.put(in_id,1);
			}
			if (out.containsKey(out_id)) {
				int totalout = out.get(out_id) + 1;
				out.put(out_id, totalout);
			}else {
				out.put(out_id,1);
			}
			//}	
		}
			
			@Override
			public void cleanup(Context context) throws IOException, InterruptedException {
	            for(Map.Entry<String, Integer> entry:in.entrySet()){
	            	String id = entry.getKey();
	            	String value = "in:" + String.valueOf(entry.getValue());
	            	
	            		context.write(new Text(id), new Text(value));
	            	}    
	            for(Map.Entry<String, Integer> entry:out.entrySet()){
	            	String id = entry.getKey();
	            	String value = "out:" + String.valueOf(entry.getValue());
	            	
	            		context.write(new Text(id), new Text(value));
	            	}      
	            } 
	           
	        }
	
	
			
			
	public static class IntReducer extends Reducer<Text, Text,Text,LongWritable>{	
		private LongWritable result = new LongWritable();
		//private Map<String,Integer> in = new HashMap<String,Integer>();
		//private Map<String,Integer> out = new HashMap<String,Integer>();
		private long v = 0;
		@Override
		public void reduce(Text key, final Iterable<Text> values, final Context context) 
				throws IOException, InterruptedException{
			long insum = 0;
			long outsum = 0;
			//String user = key.toString().substring(1);
			for (final Text value : values) {
				String record = value.toString();
				long count = Long.parseLong(record.split(":")[1]);
				if (record.startsWith("in")) {
					insum += count;
				}else if(record.startsWith("out")){
					outsum += count;
				}
			}
			
			//context.write(key, result);
		    v += insum * outsum;
			
			
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			result.set(v);
			context.write(new Text("Total"), result);
		
		}
	}
	

	@Override
	public int run(final String[] args) throws Exception{
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf,"Path Count");
		job.setJarByClass(PathCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(CountMapper.class);
		//job.setCombinerClass(IntReducer.class);
		job.setReducerClass(IntReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)? 0 : 1;
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir><output-dir>");
		}
		try {
			ToolRunner.run(new PathCount(), args);
			
		}catch (final Exception e) {
			logger.error("",e);
		}	
	}
}
