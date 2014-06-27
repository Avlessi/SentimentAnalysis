package exper;

import hbaseusage.CooccurenceTableDriver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CooccurrenceDriver {

	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, MapWritable>{	
		
		
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//File file = new File("/home/cloudera/Documents/5.txt");	
			//FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			//BufferedWriter bw = new BufferedWriter(fw);		
			
			String line = value.toString();		
			
			String [] array = line.split("[ \n\t\r]+");
			//bw.write("array_size = " + array.length);
			//for(int i = 0; i < array.length; ++i) 
			//	bw.write(" " + array[i]);
			//bw.write("\n--------");
			//bw.close();
			
			List<String> words = new ArrayList<String>();
			
//			for(int i = 0; i < array.length; i = i + 2) {
//				array[i] = normalize(array[i]);
//			}
			
			for(int i = 1; i < array.length; ) {
				if(i + 2 < array.length &&
				   (array[i].equals("R") && array[i+2].equals("A") || 
				   array[i].equals("A") && array[i+2].equals("N") || 
				   array[i].equals("V") && array[i+2].equals("T")) ) {
					
					words.add(array[i-1] + " " + array[i+1]);
					i = i + 4;
				}
				else
				{	
					words.add(array[i-1]);
					i = i + 2;
				}
			}		
			
			//find cooccurences in tweet		
			
			if(words.size() > 0) {
				
				//String keyWord = words.get(0);
				int window_size = GeneralDriver.getWindowSize();			
				//Map<String, Map<String, Integer>> bigMap = new HashMap<String, Map<String,Integer>>();			
				
				//for all words
				HashSet<String> wordset = new HashSet<String>(words);
				for(String keyword : wordset) {				
					
					ArrayList<Integer> indList = indexOfAll(keyword, words);
					
					//Map<String, Integer> mp = new HashMap<String, Integer>();
					MapWritable mp = new MapWritable();
					
					for(int ind = 0; ind < indList.size(); ++ind) {										
							
						//working inside left word window
						for(int k = 1; k <= window_size; ++k) {																		
							int pos = indList.get(ind) - k;
							if(pos >= 0 && pos < words.size()) {						
								IntWritable c = (IntWritable)mp.get(words.get(pos));					
								mp.put(new Text(words.get(pos)), c == null ? new IntWritable(1) : new IntWritable(c.get() + 1));							
							}					
						}
						
						//working inside write word position
						for(int k = 1; k <= window_size; ++k) {																		
							int pos = indList.get(ind) + k;
							if(pos >= 0 && pos < words.size()) {
								IntWritable c = (IntWritable)mp.get(words.get(pos));							
								mp.put(new Text(words.get(pos)), c == null ? new IntWritable(1) : new IntWritable(c.get() + 1));
							}
						}				
				    } 
					////write to context
					//bigMap.put(keyword, mp);
					
					context.write(new Text(keyword), mp);
							
				}
			}
			//bw.close();
	   }		
		
		
		//delete symbols which occur more than twice together 
//		private String normalize(String str) {
//			String norm_str = (str.length() > 2) ? str.substring(0, 2) : str;				
//			for(int i = 2; i < str.length(); ++i) {
//				if(str.charAt(i) != str.charAt(i-1) || str.charAt(i) != str.charAt(i-2)) {
//					norm_str = norm_str.concat(((Character)str.charAt(i)).toString());
//				}
//			}
//			return norm_str;
//		}
		
		private ArrayList<Integer> indexOfAll(String obj, List<String> list){
		    ArrayList<Integer> indexList = new ArrayList<Integer>();
		    for (int i = 0; i < list.size(); i++)
		        if(obj.equals(list.get(i)))
		            indexList.add(i);
		    return indexList;
		}
	}
	
	
	public static class CooccurrenceReducer extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			MapWritable resMap = null;
			
			
			for(MapWritable curMap : values) {
				if(resMap == null) {
					resMap = new MapWritable(curMap);
					continue;
				}
				for(Writable prim_key : curMap.keySet()) {
					if(resMap.containsKey(prim_key))
					{
						int num1 = ((IntWritable)resMap.get(prim_key)).get(); 
						int num2 = ((IntWritable)curMap.get(prim_key)).get();
						resMap.put(prim_key, new IntWritable(num1 + num2));
					}
					else
						resMap.put(prim_key, curMap.get(prim_key));				
				}			
			}		
			
			for(Writable sec_key: resMap.keySet()) {
				
				//System.out.println(key.toString() + " " + sec_key.toString());
				//context.write(key, new Text(sec_key.toString() + " " + resMap.get(sec_key).toString()));
				//addRecord(CooccurenceTableDriver.getHBaseConf(), "CooccurenceTable", key.toString(), "cooccurrence", sec_key.toString(), resMap.get(sec_key).toString());
				//addRecord(CooccurenceTableDriver.getHBaseConf(), "CooccurenceTable", key.toString(), "cooccurrence", sec_key.toString(), resMap.get(sec_key).toString());
				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Bytes.toBytes("cooccurrence"), Bytes.toBytes(sec_key.toString()), Bytes.toBytes(resMap.get(sec_key).toString()));
				System.out.println("CooccurrenceTable_cooccurrence: key = " + key.toString() + " sec_key = " + sec_key.toString() + " value = " + resMap.get(sec_key).toString());
				context.write(null, put);
			}
			
			
			//for(MapWritable mp : values) {
			//	for(Writable t : mp.keySet())
			//	context.write(key, new Text(t.toString() + " " + mp.get(t).toString()));
			//}
		}
		
//		public void addRecord(Configuration conf, String tableName, String rowKey,
//		        String family, String qualifier, String value) {
//			try {
//		        HTable table = new HTable(conf, tableName);
//		        Put put = new Put(Bytes.toBytes(rowKey));
//		        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
//		        		Bytes.toBytes(value));
//		        table.put(put);
//		        System.out.println("insert record " + rowKey + " to table "
//		                + tableName + " ok.");
//		        table.close();
//		    } catch (IOException e) {
//		         e.printStackTrace();
//		    }
//		}
	
	}
}
