package girafon.ScalableApriori;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapSumUp  
    extends Mapper<Object, Text, Text, IntWritable>{
    	
	 
	 

	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
	   
	   String[] parts = value.toString().split("\\s+");   
	   
	   
	   StringBuilder builder = new StringBuilder();
	   for(int i = 0; i < parts.length-1; i++) {
	       builder.append(parts[i] + " ");
	   }
	   
	 
	   Text candidate = new Text(builder.toString());
	   IntWritable count = new IntWritable( Integer.parseInt(parts[parts.length-1]) );  
	   
	   context.write(candidate, count);
	     
	   
	 }
}

