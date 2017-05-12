package girafon.ScalableApriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceCompress
	extends Reducer<Text,IntWritable,Text,Text> {
		

	public void reduce(Text key, Iterable<IntWritable> values,
	                Context context
	                ) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	     sum += val.get();
	   }
	   
	   for (int i = 0; i < sum; i++) {
		   context.write(key, new Text(""));
	   }
	}
}
