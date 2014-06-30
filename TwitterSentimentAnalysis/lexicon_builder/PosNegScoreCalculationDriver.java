package exper;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import emoticons.SeedWordsHelper;

public class PosNegScoreCalculationDriver {	
	
	public static class PosNegScoreCalculationMapper extends TableMapper<Text, Text> {
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {			
			String w_seed = new String(row.get());
			//System.out.println("PosNegScoreCalculationMapper: w_seed =" + w_seed);
			for(KeyValue kv : value.raw()){
				String word = new String(kv.getQualifier());
				double score = Bytes.toDouble(kv.getValue());
				context.write(new Text(word), new Text(w_seed + "," + String.valueOf(score)));
				//System.out.println("To Reducer goes: key = " + word + " value = w_seed, score which is " + w_seed + "," + score);
			}
		}
	}
	
	public static class PosNegScoreCalculationReducer extends Reducer<Text, Text, Text, Text> {   		

   	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   	 		String word = key.toString();
   	 		double sumPosScore = 0;
   	 		double sumNegScore = 0;
   	 		
   	 		for(Text text : values) {
   	 			String line = text.toString();   	 			
   	 			String [] ar = line.split(",");
   	 			String w_seed = ar[0];
   	 			if(SeedWordsHelper.isPositive(w_seed)) 
   	 				sumPosScore += Double.parseDouble(ar[1]);   	 			
   	 			else if(SeedWordsHelper.isNegative(w_seed))
   	 				sumNegScore += Double.parseDouble(ar[1]);	 			   	 			
   	 		}
   	 		//context.write(new Text(word.substring(0, 1)), new Text(word + "=" + sumPosScore + ";" + sumNegScore));
   	 		context.write(new Text(word), new Text(sumPosScore + " " + sumNegScore));
   	 	}
	}
}