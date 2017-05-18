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
	
	
	// list of all prefix in the distributed cache 
    private List<List<Integer>> prefix = new  ArrayList<List<Integer>>();
    private Trie candidateTrie;    
	
	// build candidate tree
	private void buildTree(){
		candidateTrie = new Trie(-1);
		
    	for (String line: candidate) {
    	//	System.out.println(line);
    		if (line.matches("\\s*")) continue; // be friendly with empty lines
    		// creat new prefix tempPrefix
    		List<Integer> tempPrefix = new ArrayList<Integer>();
    		String[] numberStrings = line.split("\\s+");
    		for (int i = 0; i < numberStrings.length; i++){   
    			tempPrefix.add(Integer.parseInt(numberStrings[i]));
    		}    		
    		// add p to the list of prefix
    		candidateTrie.addToTrie(tempPrefix);
    		
    	}
	}
	
 
    
	// minning: data x candidate
	// output: many tube candidate/frequent
	private void mining(Context context) throws IOException, InterruptedException{
		buildTree();
		
		// mining

		for(String line:data){
			String[] s = line.split("\\s+");
			List<Integer> t = new ArrayList<Integer>();
			for(int i=0; i<s.length; i++)
			   t.add(Integer.parseInt(s[i]));
		
			// add 1 (number of transaction)
			   t.add(1);
			
			// update support in Trie with transaction t
			candidateTrie.updateSupport(t);
		}
		
		// Output
		 List<Integer> itemset = new ArrayList<Integer>();
		 outToReducer(context, candidateTrie, itemset);
	}
	
	
	 public void outToReducer(Context context, Trie trie, List<Integer> currentPrefix) throws IOException, InterruptedException {
	      for (Integer x : trie.children.keySet()) {
	    	Trie nextTrie = trie.children.get(x);
	    	List<Integer> nextPrefix =  new ArrayList<>();
	    	
	    	for (Integer z : currentPrefix)
	    		nextPrefix.add(z);
	    	
	    	nextPrefix.add(x);
	    	
	    	// a node is a leaf if its children = nullhs
	    	if (nextTrie.children == null) {
	    		if (nextTrie.support > 0) { 
		    		Text key = new Text();
		    		key.set(itemsetToString(nextPrefix));
		    		String value = nextTrie.support + "";
		    		
		    		context.write(key, new Text(value));
	    		}
	    	}
	    	else
	    		outToReducer(context, nextTrie, nextPrefix);
	      }
	 }
	 
	 public static String itemsetToString(List<Integer> x) {
		 String a = new String();
		 a = x.get(0) + "";
		 for (int i = 1; i < x.size(); i++)
			 a = a + "\t" + x.get(i);
		 return a;
		 
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
				   System.out.println("keyA = " + keyA + ", prevKeyB =  " + prevKeyB + ", keyB = " +keyB );
				   
				   mining(context);
			   }
			   
			   
			   candidate = new ArrayList<String>();
			   
			   
			   
		   }
		   // we add candidates 
		   for (Text val : values) {
			   candidate.add(val.toString());
		   }
		   
		   
	   }
	   
	   
//	   for (Text val : values) {
//		   context.write(key, val);
//	   }		   
	   
	}
	
	// we handle the last block of Candidate
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		   if (candidate.size()>0) {
			   System.out.println("\n\n\n\n\n\nMining " + data.size() + " " + candidate.size());
			   System.out.println("keyA = " + prevKeyA + ", prevKeyB =  " + prevKeyB);
			   
			   mining(context);
		   }		 

	 }	 	
}
	