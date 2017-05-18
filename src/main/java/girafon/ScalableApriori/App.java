package girafon.ScalableApriori;
 


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 
import org.apache.hadoop.mapreduce.lib.input.*;

// MapFIM input output   alpha    beta
// ex:   output  1000  5000
// => find all prefix >=1000 but stop at <5000

// we suppose that transactions are in orderd 1 < 2 < 3 ...

public class App extends Configured implements Tool {

	
	private int numberReducers = 2;
	
	final long DEFAULT_SPLIT_SIZE = 128  * 1024 * 1024;   
	final long DEFAULT_DATA_SIZE = 128 * 1024 * 1024;  // size of a data block
	final long DEFAULT_CANDIDATE_SIZE = 16  * 1024 * 1024; // size of a candidate block   
	
   
	
	// we will output to Output/1,2,3,4
	private Path getOutputPath(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + String.valueOf(iteration));
	}
	
	
	private Path getOutputPathTemporary(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "temp" + sep +  String.valueOf(iteration));
	}	
	
	// we will output to Output/1,2,3,4
	private Path getOutputPathCandidate(Configuration conf, int iteration) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "candidate" + sep + String.valueOf(iteration));
	}	
	
	// we will output to Output/data
	private Path getOutputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	
	
	private Path getInputPathCompressData(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output") + sep + "compressedData");
	}
	
	public static Path getFrequentItemsPath(Configuration conf) {
		String sep = System.getProperty("file.separator");
		System.out.println("Getting path of frequent items");
		return new Path(conf.get("output") + sep + "1");
	}	
	
	private Path getCandidatePath(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep);
	}
	
	private Path getCandidatePathWithIteration(Configuration conf) throws IOException {
		String sep = System.getProperty("file.separator");
		return new Path(sep + conf.get("output") + sep + "candidate" + sep + conf.getInt("iteration", 1) + sep);
	}
	
	
	private Path getInputPath(Configuration conf) {
		System.out.println("Using input " + conf.get("input"));
		return new Path(conf.get("input"));
	}
	
	
	
	
	
	Configuration setupConf(String[] args, int iteration) throws IOException {
		Configuration conf = new Configuration();
		conf.set("input", args[0]);  // first step that finding all frequent itemset
		conf.set("output", args[1]);  // first step that finding all frequent itemset
		conf.setLong("block size", DEFAULT_SPLIT_SIZE);
		conf.setInt("support", Integer.valueOf(args[2]));
		conf.setInt("beta", Integer.valueOf(args[3])); // beta threshold for DApriori
				conf.setInt("iteration", iteration);  // first step that finding all frequent itemset

		// set number of block data
		if (iteration > 1) {
			int nBlockData = (int) ( (getFolderSize(getInputPathCompressData(conf), conf) / DEFAULT_DATA_SIZE) +1);
			if (nBlockData < numberReducers)
				nBlockData = numberReducers;
			conf.setInt("number block data", nBlockData);
			System.out.println("number of block data" + nBlockData);
		}
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
	

	private void addL1ToDistributedCache(Configuration conf, Job job) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(getOutputPath(conf, 1));	//récupère la liste des fichiers de sortie intermédiaires
		
		for(int i = 0; i<status.length; i++){
			if(status[i].getPath().toString().contains("part-r-")) {
				job.addCacheFile(status[i].getPath().toUri());
				System.out.println("Adding to distributed Cache: " + status[i].getPath().toString());
			}
		}
	}
	
	
	Job setupJobCandidateGeneration(Configuration conf, int k) throws Exception {
		Job job = Job.getInstance(conf, "MapFIM: preparation - Finding frequent Items");
		job.setJarByClass(App.class);
		job.setMapperClass(MapCandidateGeneration.class);
		job.setCombinerClass(CombinePreprocess.class);
		job.setReducerClass(ReduceCandidateGeneration.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		addL1ToDistributedCache(conf, job);
		
		// Input is L_k-1
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(getOutputPath(conf, k-1));	//récupère la liste des fichiers de sortie intermédiaires
		// Add only part-r-000x files
		for(int i = 0; i<status.length; i++){
			if(status[i].getPath().toString().contains("part-r-")) {
				FileInputFormat.addInputPath(job, status[i].getPath());
			}
		}
		// set output path: output/Candidate/Iteration
		FileOutputFormat.setOutputPath(job, getOutputPathCandidate(conf, k));// output path for iteration 1 is: output/1
		
		return job;
	}
	
    public long getflSize(String args) throws IOException, FileNotFoundException
    {
        Configuration config = new Configuration();
        Path path = new Path(args);
        FileSystem hdfs = path.getFileSystem(config);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long length = cSummary.getLength();
        return length;
    }
    
    // get size of the compressed Data
    public long getFolderSize(Path path, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(path) ;	//récupère la liste des fichiers de sortie intermédiaires
		
		long totalSize = 0;
		for(int i = 0; i<status.length; i++){
			totalSize += getflSize(status[i].getPath().toString());			
		}
    	return totalSize;
    }

    // get size of the compressed Data
    public boolean isStop(Configuration conf, int iteration) throws IOException {
    	return (getFolderSize(getOutputPath(conf, iteration), conf)==0);
    }
    
    
	Job setupJobPartitionData(Configuration conf, int k) throws Exception {
		// number of block candidate
		int nBlockCandidate = (int) ( (getFolderSize(getOutputPathCandidate(conf, k), conf) / DEFAULT_CANDIDATE_SIZE) +1);
		conf.setInt("number block candidate", nBlockCandidate);
		System.out.println("number of block candidate" + nBlockCandidate);

		
		Job job = Job.getInstance(conf, "MapFIM: Multiple Mappers : Partition and Duplicate, number of Block = " + conf.get("number block data"));
		job.setJarByClass(App.class);
	 
		MultipleInputs.addInputPath(job, getInputPathCompressData(conf), TextInputFormat.class, MapPartitionData.class);
		MultipleInputs.addInputPath(job, getOutputPathCandidate(conf, k), TextInputFormat.class, MapDuplicateCandidate.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setReducerClass(ReduceMiningApriori.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set output path: output/Candidate/Iteration
		FileOutputFormat.setOutputPath(job, getOutputPathTemporary(conf, k));// output path for iteration 1 is: output/1
		return job;
	}

	
	Job setupJobGetLk(Configuration conf, int k) throws Exception {
		Job job = Job.getInstance(conf, "MapFIM: Sum up to get Lk");
		job.setJarByClass(App.class);
		job.setMapperClass(MapSumUp.class);
		job.setCombinerClass(CombinePreprocess.class);
		job.setReducerClass(ReducePreprocess.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job, getOutputPathTemporary(conf, k));// output path for iteration 1 is: output/1
		FileOutputFormat.setOutputPath(job, getOutputPath(conf, k));// output path for iteration 1 is: output/1
				
		return job;
	}

	
	
	Job setupJobCompressData(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "MapFIM: Compress Data / Removing non frequent items");
		job.setJarByClass(App.class);
		job.setMapperClass(MapCompress.class);
		
		// we don't need reducers
//		job.setCombinerClass(CombinePreprocess.class);
//		job.setReducerClass(ReduceCompress.class);
		job.setNumReduceTasks(numberReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		addL1ToDistributedCache(conf, job);
		
		FileInputFormat.addInputPath(job, getInputPath(conf));

		// set output path: output/Compress
		FileOutputFormat.setOutputPath(job, getOutputPathCompressData(conf));// output path for iteration 1 is: output/1
		
		return job;
	}	
	
	public int run(String[] args) throws Exception {
	 
		// Iteration 1
		{
			Configuration conf = setupConf(args, 1);
			Job job = setupJobStep1(conf);			 
			job.waitForCompletion(true);	
		}
		
		
		// Compress Data by removing non frequent items
		{
			Configuration conf = setupConf(args, 1);
			Job job = setupJobCompressData(conf);			 
			job.waitForCompletion(true);	
		}		
			
		// Candidate generation, k = 2
 		int k = 2;
 		boolean stop = false;
 		while (!stop)
		{
			System.out.println("\n\n------------------------------------------------");

			System.out.println("-------------------Candidate Generation ---------------" + k);
			
			Configuration conf = setupConf(args, k);
			Job job = setupJobCandidateGeneration(conf, k);			 
			job.waitForCompletion(true);			

			
			// verify if there is Candidate or Not
			if (getFolderSize(getOutputPathCandidate(conf, k), conf) == 0){
				stop = true;
			}
			else {

				// Multi Mappers 
				// Mapper1: Partition data, Mapper2: Duplicate Candidate
			 
				System.out.println("-------------------Mapper1+2: Partition Data AND Duplicate Candidate---------------" + k);
				
				  conf = setupConf(args, k);
				  job = setupJobPartitionData(conf, k);			 
				job.waitForCompletion(true);			
			 
	
			
				// Getting Lk
	 
				System.out.println("-------------------Sum up to get Lk---------------" + k);
				
				  conf = setupConf(args, k);
				  job = setupJobGetLk(conf, k);			 
				job.waitForCompletion(true);
				
				stop = isStop(conf, k);
				k++;
			}
			
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




