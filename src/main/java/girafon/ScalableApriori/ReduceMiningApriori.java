package girafon.ScalableApriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceMiningApriori  
	extends Reducer<Text,Text,Text,Text> {
		
	
	public void reduce(Text key, Iterable<Text> values,
	                Context context
	                ) throws IOException, InterruptedException {
	   
	   for (Text val : values) {
		   context.write(key, val);
	   }		   
	   
	}
}
	
