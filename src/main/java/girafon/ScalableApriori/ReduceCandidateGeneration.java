package girafon.ScalableApriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class ReduceCandidateGeneration 
	extends Reducer<Text,IntWritable,Text,Text> {
	
	
	private int count;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("---------------------REDUCER for CANDIDATE GENERATION--------------");
		Configuration config = context.getConfiguration();
		count 	= config.getInt("iteration", 0);		
		return;
	}
	public void reduce(Text key, Iterable<IntWritable> values,
	                Context context
	                ) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	     sum += val.get();
	   }
	   
	   // is it a TRUE candidate 
	   if (sum == count) {
		   context.write(key, new Text(""));
	   }
	}
}
