package girafon.ScalableApriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class ReducePreprocess
	extends Reducer<Text,IntWritable,Text,Text> {
	private IntWritable result = new IntWritable();

	private int support;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	
		
		
		Configuration config = context.getConfiguration();
		support = config.getInt("support", 1);
			
		return;
	}
	public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	     sum += val.get();
	   }
	   
	   if (sum >= support) {
		   result.set(sum);
		   context.write(key, new Text(""+sum));
	   }
	}
}
