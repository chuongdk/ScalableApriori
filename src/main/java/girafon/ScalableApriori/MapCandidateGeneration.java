package girafon.ScalableApriori;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class MapCandidateGeneration 
extends Mapper<Object, Text, Text, IntWritable>{
	
	 private final static IntWritable one = new IntWritable(1);
	 private Text word = new Text();
	 private List<String> L1 = new ArrayList<String>();
	
	  private void getCache(Context context) throws IOException {
			 // Read file from distributed caches - each line is a item/itemset with its frequent
			Configuration config = context.getConfiguration();		
		
			// URI to locate cachefile, ex URI a = new URI("http://www.foo.com");
			List<URI> uris = Arrays.asList(context.getCacheFiles());	
			System.out.println("Reading cached files");
			
			
			for (URI uri : uris) {
				Path p = new Path(uri);
				//System.out.println("Loading " + uri.toString());
				FileSystem fs = FileSystem.get(context.getConfiguration());
				InputStreamReader ir = new InputStreamReader(fs.open(p));
				BufferedReader data = new BufferedReader(ir);
		    	while (data.ready()) {    		
		    		String line=data.readLine();
		    		if (line.matches("\\s*")) continue; // be friendly with empty lines
		    		String item = line.substring(0, line.indexOf('\t'));
		    		L1.add(item);
		    	}  
			}
			return;
	   }	 

	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		//System.out.println("Mapper getting Cache");
		getCache(context);			
	}	  
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
	   StringTokenizer itr = new StringTokenizer(value.toString());
	   while (itr.hasMoreTokens()) {
	     word.set(itr.nextToken());
	     
	     
	     context.write(word, one);
	   }
	 }
}

