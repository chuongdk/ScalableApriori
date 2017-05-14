package girafon.ScalableApriori;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapDuplicateCandidate 
	extends Mapper<Object, Text, Text, Text>{
	
	private Integer mapID;
	private int nBlock;
	
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] parts = context.getTaskAttemptID().getTaskID().toString().split("_");
		System.out.println("-----------------MAPPER DUPLICATE CANDIDATE number -----------------");
		System.out.println(parts[parts.length - 1]);
		System.out.println("-------------------------------------------------------");		
		mapID = Integer.parseInt(parts[parts.length - 1]);
		mapID++;  // so mapID > 0
		nBlock = context.getConfiguration().getInt("number block data", 0);
	}
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
 
	
		for (Integer i = 0; i < nBlock; i++) {
			context.write(new Text(i.toString() + " " + mapID.toString()), value);
		}
		 
		 
		  		 
	 }
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 
 
	 }	 
}
