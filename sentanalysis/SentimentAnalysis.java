package sentanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SentimentAnalysis {
	public static class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			RunTagger rt = new RunTagger();
			String resTokens = "";
			try {
				resTokens = rt.runTagger(value.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("IOException in tagger!!!!!");
			}
			System.out.println("resTokens = " + resTokens);
			
			String [] array = resTokens.split("[ \n\r\t]+");
			for(int i = 0; i < array.length; ++i)
				array[i] = normalize(array[i].toLowerCase());
			
			
			List<String> words = new ArrayList<String>();			
			
			for(int i = 1; i < array.length; ) {
				if(i + 2 < array.length &&
				   (array[i].equals("R") && array[i+2].equals("A") || 
				   array[i].equals("A") && array[i+2].equals("N")) || 
				   array[i].equals("V") && array[i+2].equals("T")) {
					
					words.add(array[i-1] + " " + array[i+1]);
					i = i + 4;
				}
				else
				{	
					words.add(array[i-1]);
					i = i + 2;
				}
			}
			
			double sentiment_score = 0;
			//HTable table = new HTable(SentimentAnalysisDriver.getHBaseConfiguration(), "ScoreTable");
			HTable table = new HTable(SentimentAnalysisDriver.getHBaseConfiguration(), "GraphTable");
			for(int i = 0; i < words.size(); ++i) {				
				Get get = new Get(Bytes.toBytes(words.get(i)));
				get.addColumn(Bytes.toBytes("score"), Bytes.toBytes(words.get(i)));
				//get.addColumn(Bytes.toBytes("alpha"), Bytes.toBytes(words.get(i)));
				Result rs = table.get(get);
				if(!rs.isEmpty()){	
					sentiment_score += Bytes.toDouble(rs.getValue(Bytes.toBytes("score"), Bytes.toBytes(words.get(i))));										
					//sentiment_score += Bytes.toDouble(rs.getValue(Bytes.toBytes("alpha"), Bytes.toBytes(words.get(i))));
				}
			}
			table.close();			
			
			context.write(new Text(value), new Text(sentiment_score > 0 ? "Positive" : "Negative"));
		}
		//delete symbols which occur more than twice together 
		private String normalize(String str) {
			String norm_str = (str.length() > 2) ? str.substring(0, 2) : str;				
			for(int i = 2; i < str.length(); ++i) {
				if(str.charAt(i) != str.charAt(i-1) || str.charAt(i) != str.charAt(i-2)) {
					norm_str = norm_str.concat(((Character)str.charAt(i)).toString());
				}
			}
			return norm_str;
		}
	}
}
