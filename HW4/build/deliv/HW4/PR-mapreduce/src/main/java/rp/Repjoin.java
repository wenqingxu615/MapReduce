package rp;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Repjoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Repjoin.class);
    private static final String COMMA_SEPARATOR = ",";
    private static final Long MAX_Length = 200000L;

    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
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
                    // parse through the edges.csv, store the (user, set of follower)
                    // in the hashmap
                    while ((line = rdr.readLine()) != null) {
                        String[] nodes = line.split(COMMA_SEPARATOR);

                        String x = nodes[0];
                        String y = nodes[1];

                        if (Long.valueOf(x) >= MAX_Length || Long.valueOf(y) >= MAX_Length)
                            continue;
                        // use the user to be the key in the hashmap, 
                        // and its value is the set of followers who follow this user.
                        nodeMapping.computeIfAbsent(y, node -> new HashSet<>()).add(x);
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

        /*
         * This map class basically use the hashmap we create twice
         * to join the edges twice.
         * In short, everytime when we get a edge (x,y), we look up 
         * x's followers fo_x. Then, look up the followers of fo_x.
         * If the followers of fo_x contains y, we can add the counter by 1.
         */
        
        @Override
        public void map(Object key, Text value, Context context) {
            // read the edges.csv again
        	String[] nodes = value.toString().split(COMMA_SEPARATOR);
            
            String x = nodes[0];
            String y = nodes[1];

            if (Long.valueOf(x) >= MAX_Length || Long.valueOf(y) >= MAX_Length)
                return;

            // x is now the follower of y
            // we need to join two paths to find path2
            // so what we are looking for is the followers of the follower
            // As the result, we get this Set<String> called followers_of_x
            Set<String> followers_of_x = nodeMapping.get(x);
            
            // if followers_of_x set contains the user y
            if (CollectionUtils.isNotEmpty(followers_of_x)) {
                // increment counter
                for (String fo_x : followers_of_x) {
                	// for each fo_x in the follower_of_x set
                	// if its follower equals to current user followed by x
                	// it means that we find a point that can join the edges to a triangle
                    Set<String> joinpoints = nodeMapping.get(fo_x);
                    if (CollectionUtils.isNotEmpty(joinpoints) && joinpoints.contains(y)) {
                        context.getCounter(CounterEnum.TRIANGLE_COUNTER).increment(1);
                    }
                }
            }
        }
    }

    /**
     * @param args - input values
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new Repjoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


    
    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Rep Join Analysis");
        job.setJarByClass(Repjoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", COMMA_SEPARATOR);

        job.setMapperClass(EdgeMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[0] + "/edges.csv"));

        boolean isComplete = job.waitForCompletion(true);

        Counters cn = job.getCounters();
        Counter c1 = cn.findCounter(CounterEnum.TRIANGLE_COUNTER);
        // divide the answer by 3 because there are duplicates
        logger.info("Path 2 edge count:" + c1.getDisplayName() + ":" + c1.getValue() / 3);

        return isComplete ? 0 : 1;

    }
}
