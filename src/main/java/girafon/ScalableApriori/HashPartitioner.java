package girafon.ScalableApriori;

import org.apache.hadoop.mapreduce.Partitioner;



public class HashPartitioner<K, V> extends Partitioner<K, V> {

	@Override
	public int getPartition(K arg0, V arg1, int arg2) {
		// TODO Auto-generated method stub
	
		// key is a list of items in a prefix, 
		String key = arg0.toString();
		
		// now get the list
		
		String[] numberStrings = key.split("\\s+");
		
		
		int	newKey = Integer.parseInt(numberStrings[0]);
		
		
				
		return newKey % arg2;
	}
  
}