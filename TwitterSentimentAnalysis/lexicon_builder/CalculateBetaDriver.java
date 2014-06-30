package exper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.math.BigDecimal;

public class CalculateBetaDriver {
	
	public static class CalculateBetaMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String [] words = line.split("\t");
			String [] sentence = words[1].split("=");
			String [] scores = sentence[1].split(";");
			String posScore = scores[0];
			String negScore = scores[1];
			
			context.write(new Text(words[0]), new Text(posScore + ";" + negScore));		
		}
	}
	
	public class CalculateBetaReducer extends Reducer<Text, Text, Text, Text> {
		  
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sumPos = 0;
			double sumNeg = 0;
			for(Text word: values) {
				String [] scores = word.toString().split(";");
				sumPos += Double.valueOf(scores[0]);
				sumNeg += Double.valueOf(scores[1]);				
			}
			context.write(key, new Text(sumPos + ";" + sumNeg));
		}
	}
}
