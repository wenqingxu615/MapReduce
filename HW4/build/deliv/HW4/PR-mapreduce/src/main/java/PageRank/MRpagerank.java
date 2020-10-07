package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

//import org.apache.hadoop.io.DoubleWritable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MRpagerank extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(MRpagerank.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final String BLANK_SEPARATOR = "\t";
    private static final Double alpha = 0.0;
    private static Integer k = 1000;
    private static Double initialPR = (double) 1/(k*k);
    //private static Integer totalnode = 100;
    
    private static class pageMapper extends Mapper<Object, Text, Text, Text>{
    	private Map<String, Set<String>> nodeMapping = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            BufferedReader rdr = null;

            try {
                URI[] files = context.getCacheFiles();
                logger.info("Files are" + Arrays.toString(files));

                if (files == null || files.length == 0) {
                    logger.info("File not found");
                    throw new FileNotFoundException("Edge information is not set in DistributedCache");
                }

                // Read all files in the DistributedCache
                for (URI u : files) {
                    String filename;
                    int lastindex = u.toString().lastIndexOf('/');
                    if (lastindex != -1) {
                        filename = u.toString().substring(lastindex + 1);
                    } else {
                        filename = u.toString();
                    }

                    rdr =  new BufferedReader(new FileReader(filename));

                    String line;
                    while ((line = rdr.readLine()) != null) {
                        String[] nodes = line.split(COMMA_SEPARATOR);

                        String x = nodes[0];
                        String y = nodes[1];
                        nodeMapping.computeIfAbsent(x, node -> new HashSet<>()).add(y);
                    }
                }
            } catch (Exception e) {
                logger.info("Error occured while creating cache: " + e.getMessage());
            } finally {
                if (rdr != null) {
                    rdr.close();
                }
            }
        }
        
    	@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			 
			 String[] nodes = value.toString().split(COMMA_SEPARATOR);
			 String from = nodes[0];
			 String to = nodes[1];
			 if (Integer.parseInt(from) % k == 1 ) {
				 context.write(new Text(from), new Text("0.0"));
			 }
			 Set<String> values = nodeMapping.get(from); 
			 for (String newvalue : values) {
				 context.write(new Text(to), new Text(newvalue));
    	}	
    }
    }
    
    private static class PageRankReducer extends Reducer<Text, Text,Text,Text>{
    	private Map<String,Double> rankmap = new HashMap<String,Double>();
    	
    	@Override
    	public void reduce(Text key, final Iterable<Text> values, final Context context) 
				throws IOException, InterruptedException{
    		double sum = 0.0;
    		for (Text value: values) {
    			sum += Double.parseDouble(value.toString());
    		}
    	rankmap.put(key.toString(),sum);
    	}
    	
    	@Override
    	public void cleanup(Context context) throws IOException, InterruptedException {
    		Double delta = rankmap.get("0");
    		logger.info("*****************"+ "delta(page rank loss): "+ delta + "**********************");
    		Double total = 0.0;
    		for(Map.Entry<String, Double> entry:rankmap.entrySet()){
    			if (!entry.getKey().equals("0")) {
    				String newkey = entry.getKey();
    				Double newval = entry.getValue();
    				Double newpagerank = alpha * initialPR + (1 - alpha) * (newval + delta * initialPR);
    				context.write(new Text(newkey), new Text(newpagerank.toString()));
    			}
    		//logger.info("total pagerank:"+ total);
    		}
    	}
    }
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
    		throw new Error("Two arguments required:\n<input-dir><output-dir>");
    	}
    	try {
    		ToolRunner.run(new MRpagerank(), args);
    		
    	}catch (final Exception e) {
    		logger.error("",e);
    	}
    }
    
    @Override
	public int run(final String[] args) throws Exception{
    	String inputPath = args[0]+"/edges.txt";
		String outputPath = args[1];
		String cache = args[0] + "/pagerank.txt";
		//k = args[2];
		
		for (int i = 1; i <=10 ; i++) {
			final Configuration conf = getConf();
			final Job job = Job.getInstance(conf,"MRpagerank");
			
			
			//jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
			myMapReduceTask(job, inputPath, outputPath + i,cache);
			cache = outputPath + i + "/part-r-00000";
			//deleteFolder(conf, outputPath + (i-1));
		}
		return 1;
    }
    
    private static void myMapReduceTask(Job job, String inputPath, String outputPath, String cache) 
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    	final Configuration jobConf = job.getConfiguration();
    	jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);
    	job.setJarByClass(MRpagerank.class);
    	
    	job.setMapperClass(pageMapper.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	
    	job.setReducerClass(PageRankReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setNumReduceTasks(1);
    	FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		NLineInputFormat.setNumLinesPerSplit(job, 50000);
		job.setInputFormatClass(NLineInputFormat.class);
		job.addCacheFile(new URI(cache));
    	while(!job.waitForCompletion(true)) {}

		return;
    }
    }


