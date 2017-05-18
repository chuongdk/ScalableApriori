package girafon.ScalableApriori;
import java.io.IOException;

import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

 
import java.net.URI;
 
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class MapCandidateGeneration 
extends Mapper<Object, Text, Text, IntWritable>{
	
	 private final static IntWritable one = new IntWritable(1);
	 private Text word = new Text();
	 private List<Integer> l1 = new ArrayList<Integer>();
	
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
		    		l1.add(Integer.parseInt(item));
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
		 // Mapper get Lk, then join Lk x L1
		 List<Integer>  lk = new ArrayList<Integer>();
		 
 		 StringTokenizer itr = new StringTokenizer(value.toString());
 		 while (itr.hasMoreTokens()) {
 			 lk.add(Integer.parseInt(itr.nextToken().toString()));
 		 }
 		 
 		 // remove the frequency
 		 lk.remove(lk.size()-1);

 		 for (Integer x:l1) {
 			 if (!lk.contains(x)) {
 				List<Integer> candidate = new ArrayList<Integer>(lk);
 				candidate.add(x);
 				Collections.sort(candidate);
 				String out = String.join(" ", candidate); 				
 				word.set(out);
 				context.write(word, one);
 			 }
 		 }
 		 
 		  		 
	 }
}

