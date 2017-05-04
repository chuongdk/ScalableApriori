package girafon.MapFIM;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// MapFIM input output   alpha    beta
// ex:   output  1000  5000
// => find all prefix >=1000 but stop at <5000

// we suppose that transactions are in orderd 1 < 2 < 3 ...

public class App extends Configured implements Tool {

	private long maxCandidate = 10000000;

	private int maxfullBetaPrefix = 500000; // each time we send only 10000 beta Prefix to sold
	
	private int numberReducers = 2;
	
	final long DEFAULT_SPLIT_SIZE = 2  * 1024 * 1024;  // 1M
	
	
	
	
	private long maxCache = maxCandidate;
	
	

	
	
	
	
	
	private ArrayList<Path> queuePrefix = new ArrayList<Path>();
	private ArrayList<Path> queueCandidate = new ArrayList<Path>();
	
	
	// List that contain all prefix with alpha <= support < beta

    private List<List<Integer>> fullBetaPrefix = new  ArrayList<List<Integer>>();
    
    private List<List<Integer>> betaPrefix = new  ArrayList<List<Integer>>();
    


    
	
	// we will output to Output/1,2,3,4
	private Path getOutputPath(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		System.out.println("Uls"
				+ "sing output " + conf.get("output") + sep + String.valueOf(iteration));
		
		return new Path(conf.get("output") + sep + String.valueOf(iteration));
	}
	
	// we will output to Output/data
	private Path getOutputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	
	
	private Path getInputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		System.out.println("Using input " + conf.get("output") + sep + "compressedData");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	
	private Path getCandidatePath(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep);
	}
	
	private Path getCandidatePathWithIteration(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep + conf.getInt("iteration", 1));
	}
	
	
	private Path getInputPath(Configuration conf) {
		System.out.println("Using input " + conf.get("input"));
		return new Path(conf.get("input"));
	}
	
	
	
	
	
	Configuration setupConf(String[] args, int iteration) {
		Configuration conf = new Configuration();
		conf.set("input", args[0]);  // first step that finding all frequent itemset
		conf.set("output", args[1]);  // first step that finding all frequent itemset
		
		conf.setInt("support", Integer.valueOf(args[2]));
		conf.setInt("beta", Integer.valueOf(args[3])); // beta threshold for DApriori
				conf.setInt("iteration", iteration);  // first step that finding all frequent itemset

		conf.setLong(
			    FileInputFormat.SPLIT_MAXSIZE,
			    DEFAULT_SPLIT_SIZE);
		
		return conf;
	}
	
	Job setupJobStep1(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "MapFIM: preparation - Finding frequent Items");
		job.setJarByClass(App.class);
		job.setMapperClass(MapPreprocess.class);
		job.setCombinerClass(CombinePreprocess.class);
		job.setReducerClass(ReducePreprocess.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, getInputPath(conf));
		FileOutputFormat.setOutputPath(job, getOutputPath(conf, 1));// output path for iteration 1 is: output/1
		
		return job;
	}
	

	Job setupJobStep1B(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "Compress Data");
		job.setJarByClass(App.class);
		job.setMapperClass(MapCompress.class);
		
		job.setCombinerClass(CombinePreprocess.class);
		job.setReducerClass(ReducePreprocess.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(numberReducers);

		
		FileInputFormat.addInputPath(job, getInputPath(conf));
		FileOutputFormat.setOutputPath(job, getOutputPathCompressData(conf));// output path for iteration 1 is: output/1
		return job;
	}
	
		
	public int run(String[] args) throws Exception {
		if (Integer.parseInt(args[4]) > 0)
			maxfullBetaPrefix = Integer.parseInt(args[4]);
		
		// Iteration 1
		{
			Configuration conf = setupConf(args, 1);
			Job job = setupJobStep1(conf);			 
			job.waitForCompletion(true);	
 
		}
		
 
		// Iteration 1b - to compress data 
		 {
			// Now, Queue contains all frequent items

			System.out.println("__________________STEP 1B _____________________");
			System.out.println("__________________STEP 1B _____________________");
			
			Configuration conf = setupConf(args, 0);
			conf.setInt("support", 1);  // because we compress data
			Job job = setupJobStep1B(conf);	
			
			// add frequent items to distributed cache
			//addCacheFilesFromQueue(conf, job, false);
			
			job.waitForCompletion(true);
		}		
		 
		

		return 1;
	}
		
	public static void main(String[] args) throws Exception {
		
		long beginTime = System.currentTimeMillis();

		int exitCode = ToolRunner.run(new App(), args);

		long endTime = System.currentTimeMillis();
		
		
		System.out.println("support : " + args[2]);
		System.out.println("beta : " + args[3]);
		System.out.println("Total time : " + (endTime - beginTime)/1000 + " seconds.");
		
		System.exit(exitCode);
	}

}















