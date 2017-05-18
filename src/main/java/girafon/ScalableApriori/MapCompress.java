package girafon.ScalableApriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapCompress 
extends Mapper<Object, Text, Text, Text>{
	

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
		 List<Integer>  t = new ArrayList<Integer>();
		 
		 StringTokenizer itr = new StringTokenizer(value.toString());
		 while (itr.hasMoreTokens()) {
			 Integer item = Integer.parseInt(itr.nextToken().toString());
			 if (l1.contains(item))
				 t.add(item);
		 }
		 
		
		 if (t.size() > 1) {
			 	// transaction are sorted
			 	Collections.sort(t);
				//String out = String.join(" ", t);
				
				StringBuilder builder = new StringBuilder();
				for(Integer s : t) {
				    builder.append(s + " ");
				}
				word.set(builder.toString());
				context.write(word, new Text(""));
		 }
		 
		  		 
	 }
}

