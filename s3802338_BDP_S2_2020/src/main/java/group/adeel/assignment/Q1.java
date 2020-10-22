package group.adeel.assignment;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

public class Q1 {

	// Apache Logger instance
	private static final Logger LOG = Logger.getLogger(Q1.class);

	// Mapper
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Log for mapper
			LOG.setLevel(Level.INFO);
			LOG.info("The mapper task of Adeel Ahmed, s3802338");
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// Saving word length to a variable
				int wordLength = itr.nextToken().length();
				String wordSize = "";

				// assigning size to each word based on their length
				if (wordLength >= 1 && wordLength <= 4) 
					wordSize = "small";
				else if (wordLength >= 5 && wordLength <=7) 
					wordSize = "medium";
				else if (wordLength >= 8 && wordLength <=10) 
					wordSize = "long";
				else  
					wordSize = "extra-long";

				word.set(wordSize);
				context.write(word, one);
			}
		}
	}

	//Reducer
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

	//Main Function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Creating job
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Q1.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}