package girafon.ScalableApriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ReduceMiningApriori  
	extends Reducer<Text,Text,Text,Text> {
		
	private int prevKeyA = -1;
	private int prevKeyB = -1;
	
	List<String> data = new ArrayList<String>();
	List<String> candidate = new ArrayList<String>();
	
	
	
	
	// build candidate tree
	private void buildTree(){
		
	}
	
	
	// minning: data x candidate
	// output: many tube candidate/frequent
	private void mining(){
		
	}
	
	
	
	
	// key: A space B. A = #block, B=0,1,2,3....
	// B = 0: it is data, B > 0: it is candidate
	public void reduce(Text key, Iterable<Text> values,
	                Context context
	                ) throws IOException, InterruptedException {
	   int keyA, keyB;
	   String[] parts = key.toString().split(" ");
	   keyA =  Integer.parseInt(parts[0]);
	   keyB =  Integer.parseInt(parts[1]);
	   
	   
	   // we handle Data
	   if (keyB == 0) {
		   // we move to other block of Data. So we clear old Data
		   if (keyA != prevKeyA) {
			   data = new ArrayList<String>();   
		   }
		   // we add transaction to data 
		   for (Text val : values) {
			   data.add(val.toString());
		   }
		   
	   }

	   
	   // we handle Candidate
	   if (keyB > 0) { 
		   // we move to other block of Candidate
		   if (keyB != prevKeyB){
			   // we mine Data x Candidate
			   if (candidate.size()>0) {
				   System.out.println("\n\n\n\n\n\nMining " + data.size() + " " + candidate.size());
			   }
			   
			   
			   candidate = new ArrayList<String>();
			   
			   
			   
		   }
		   // we add candidates 
		   for (Text val : values) {
			   candidate.add(val.toString());
		   }
		   
		   
	   }
	   
	   
	   for (Text val : values) {
		   context.write(key, val);
	   }		   
	   
	}
	
	// we handle the last block of Candidate
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 

	 }	 	
}
	
