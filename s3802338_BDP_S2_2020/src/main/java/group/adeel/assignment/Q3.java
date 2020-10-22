package group.adeel.assignment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Q3 {
	// Apache Logger instance
	private static final Logger LOG = Logger.getLogger(Q3.class);
	
	// Mapper
	public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {

		private static Map<String, Integer> mp;
		private static final int MAP_FLUSH_SIZE = 500;
		
		// create map instance if not created
		public static Map<String, Integer> getInstance() {
			if(mp == null)
				mp = new HashMap<String, Integer>();
			return mp;
		}

		// Map Function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Log for mapper
			LOG.setLevel(Level.INFO);
			LOG.info("The mapper task of Adeel Ahmed, s3802338");
			
			// Initialize map instance
			Map<String, Integer> mp = getInstance();
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			// if word is already in map add +1 to its current value else assign one as the value and word as the key to map
			String map_key = "";
			while (itr.hasMoreTokens()) {
				 map_key = itr.nextToken();
				if(mp.containsKey(map_key))
					mp.put(map_key, mp.get(map_key) + 1);
				else 
					mp.put(map_key, 1);
			}  
			flush_map(context, false);
		}

		// Flushing the map if it becomes too large to avoid our mapper from crashing
		private void flush_map(Context context, boolean flush) throws IOException, InterruptedException {
			// Initialize map instance
			Map<String, Integer> mp = getInstance();
			
			if(!flush) 
				if(mp.size() < MAP_FLUSH_SIZE)
					return;
			
			// Implementing in-mapper combiner instead of independent combiner
			Iterator<Map.Entry<String, Integer>> it = mp.entrySet().iterator();
			while(it.hasNext()) {
				Map.Entry<String, Integer> mpPair = it.next();
				context.write(new Text(mpPair.getKey()), new IntWritable(mpPair.getValue().intValue()));
			}
			// clearing the map
			mp.clear(); 
		}

		//Overriding Mapper clean up method
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			flush_map(context, true); 
		}
	}

	// Reducer 
	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// Log for reducer
			LOG.info("The reducer task of Adeel Ahmed, s3802338");
			
			int sum = 0;
			for (IntWritable val : values) 
				sum += val.get();
			result.set(sum);
			context.write(key, result);
		}
	}

	// Main Function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Creating job
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Q3.class);
		
		// without independent Combiner
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ReducerClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}