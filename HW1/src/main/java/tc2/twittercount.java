package tc2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class twittercount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(twittercount.class);
	
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
		private Map<String,Integer> map;
	 /*Instead of adding "1" and do the aggregation later,
	 do in-mapper combining using a hashmap	
	 */
		public void setup(Context context) throws IOException, InterruptedException{
			map = new HashMap<String,Integer>();
		}
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			int counter = 0;
			while (itr.hasMoreTokens()) { 
				String id = itr.nextToken();
				counter += 1;
				//the second id in each edge is our key for transferring to reducer
				if (counter%2 == 0) {
					//If the id has been in the map, value += 1
					if (map.containsKey(id)) {
						int total = map.get(id) + 1;
						map.put(id, total);
					//else put (id,1) in the map
					}else {
						map.put(id,1);
					}
				}	
			}
		}
			
			//scan the map and write each pair of (id,value) to context
			@Override
			public void cleanup(Context context) throws IOException, InterruptedException {
	            //send the key-value after aggregation
	            for(Map.Entry<String, Integer> entry:map.entrySet()){
	                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));                 
	            }   
	        }
	}
	
	
			
			
	public static class IntReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
	//Reducer is the same as the one in wordcount program	
		private IntWritable result = new IntWritable();
		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) 
				throws IOException, InterruptedException{
			int sum =0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			
			context.write(key, result);
		} 
		
	}
	@Override
	public int run(final String[] args) throws Exception{
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf,"Twitter Count");
		job.setJarByClass(twittercount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntReducer.class);
		job.setReducerClass(IntReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)? 0 : 1;
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir><output-dir>");
		}
		try {
			ToolRunner.run(new twittercount(), args);
			
		}catch (final Exception e) {
			logger.error("",e);
		}	
	}
}
