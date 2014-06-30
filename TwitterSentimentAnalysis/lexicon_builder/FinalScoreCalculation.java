package exper;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FinalScoreCalculation {

	public static class FinalScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String [] words = line.split("\t");			
			
			context.write(new Text(words[0]), new Text(words[1]));
		}
	
		public static class FinalScoreReducer extends TableReducer<Text, Text, ImmutableBytesWritable>  {   		
			@Override
	   	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	 		
	   	 		for(Text text : values) {
	   	 			Put put = new Put(key.getBytes());
	   	 			String word = text.toString();
	   	 			String [] sentence = word.split("=");
	   	 			String [] scores = sentence[1].split(";");
	   	 			String posScore = scores[0];
	   	 			String negScore = scores[1];
	   	 			double beta = GeneralDriver.getBeta();
	   	 			double finScore = Double.valueOf(posScore) - beta * Double.valueOf(negScore);
	   	 			put.add(Bytes.toBytes("score"), Bytes.toBytes(sentence[0]), Bytes.toBytes(finScore));
	   	 			context.write(null, put);
	   	 		}
	   	 	}
		}
	}
}
