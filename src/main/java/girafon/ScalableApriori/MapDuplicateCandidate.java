package girafon.ScalableApriori;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapDuplicateCandidate 
	extends Mapper<Object, Text, Text, Text>{
	
 
	private int nBlockData;
	private int nBlockCandidate;
	private Random rand = new Random();
	
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] parts = context.getTaskAttemptID().getTaskID().toString().split("_");
		System.out.println("-----------------MAPPER DUPLICATE CANDIDATE number -----------------");
		System.out.println(parts[parts.length - 1]);
		System.out.println("-------------------------------------------------------");		
		 
		nBlockData = context.getConfiguration().getInt("number block data", 0);
		nBlockCandidate = context.getConfiguration().getInt("number block candidate", 0);
	}
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
 
		 Integer keyRandom = rand.nextInt(nBlockCandidate) + 1;
		 
		for (Integer i = 0; i < nBlockData; i++) {
			context.write(new Text(i.toString() + " " + keyRandom.toString()), value);
		}
		 
		 
		  		 
	 }
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 
 
	 }	 
}

