package girafon.ScalableApriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.util.Random;
public class MapPartitionData 
extends Mapper<Object, Text, Text, Text>{
	
	private Integer mapID;
	private Integer nBlock = 0;
	private Random rand = new Random();

	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
//		String[] parts = context.getTaskAttemptID().getTaskID().toString().split("_");
//		System.out.println("-----------------MAPPER PARTITION Data number -----------------");
//		System.out.println(parts[parts.length - 1]);
//		System.out.println("-------------------------------------------------------");		
//		mapID = Integer.parseInt(parts[parts.length - 1]);
		nBlock = context.getConfiguration().getInt("number block data", 0);
	}
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {

//			Configuration			conf = context.getConfiguration();
//		    Long blockSize = conf.getLong("block size", 0);	//on récupère la taille d'un bloc afin de savoir dans quel bloc on se trouve
//		    Integer currentBlock = (int)((((long)key.get())/blockSize)+1);	//key étant l'octet de début de ligne, on peut savoir dans quel bloc on se trouve	 
		    

		 Integer keyRandom = rand.nextInt(nBlock);
		
		context.write(new Text(keyRandom.toString() + " 0"), value);
		 
		 
		  		 
	 }
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 
 
	 }	 
}

