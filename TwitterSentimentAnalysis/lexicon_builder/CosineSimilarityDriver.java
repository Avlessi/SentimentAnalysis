package exper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.hbase.util.Bytes;

public class CosineSimilarityDriver {
	
	public final static double theta = 0.01;
	
	public static class CosineSimilarityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {		
	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
//			File file = new File("/home/cloudera/Documents/1.txt");	
//			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
//			BufferedWriter bw = new BufferedWriter(fw);		
			
//			String line = value.toString();		
//			String year = line.substring(0, 4);
//			bw.write(year + "\n");
//			bw.close();		
			
//			double airTemperature = Integer.parseInt(line.substring(5,7));
//			context.write(new Text(year), new DoubleWritable(airTemperature));			
			
			String line = value.toString();
			String [] sentence = line.split("\t"); //value.toString gives a whole line. Here we extract exact value  
			//System.out.println("sentence_1");
			//System.out.println(sentence[1]);
			String [] array = sentence[1].split("///");
//			System.out.println("array_size = " + array.length);
//			for(int i = 0; i < array.length; ++i)
//				System.out.print(array[i] + " ");
			
			Map<String, Double> map = new HashMap<String, Double>();
			for(int i = 0; i < array.length; ++i) {
				String [] words  = array[i].split(",");
				//System.out.println("arrrr = " + array[i]);
				if(words.length >= 2)
					map.put(words[0], Double.parseDouble(words[1]));
			}
			//Iterator<String> it1 = map.keySet().iterator();
			//Iterator<String> it2 = map.keySet().iterator();
			for(String w_u: map.keySet()) {
				for(String w_v: map.keySet()) { 
					if(!w_u.equals(w_v)) {
						double norm_u_c = map.get(w_u);
						double norm_v_c = map.get(w_v);
						
						context.write(new Text(w_u + "," + w_v), new DoubleWritable(norm_u_c * norm_v_c));
						//System.out.println("CosineSimilarityMapper: w_c = " + sentence[0] + " w_u = " + w_u + " w_v = " + w_v +  " norm_u_c = " + norm_u_c + " norm_v_c = " + norm_v_c + " cos_sim_c = " + norm_u_c * norm_v_c + " To the reducer goes: key = " + w_u + "," + w_v + " val = " + norm_u_c * norm_v_c );
					}
				}
			}
			
			
			
			/*List<String> list1 = new ArrayList<String>(map.keySet());
			List<String> list2 = new ArrayList<String>(list1);
			for(int i = 0; i < list1.size(); ++i) {
				String w_u = list1.get(i);
				for(int j = i + 1; j < list2.size(); ++j) {					
					String w_v = list2.get(j);
					
					double norm_u_c = map.get(w_u);
					double norm_v_c = map.get(w_v);
					
					context.write(new Text(w_u + "," + w_v), new DoubleWritable(norm_u_c * norm_v_c));
					System.out.println("CosineSimilarityMapper: w_c = " + sentence[0] + " w_u = " + w_u + " w_v = " + w_v +  " norm_u_c = " + norm_u_c + " norm_v_c = " + norm_v_c + " cos_sim_c = " + norm_u_c * norm_v_c + " To the reducer goes: key = " + w_u + "," + w_v + " val = " + norm_u_c * norm_v_c );
				}
			}*/			
		}	
	}
	
	public static class CosineSimilarityReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable>  {   		

   	 	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
//   	    		int i = 0;
//   	    		for (IntWritable val : values) {
//   	    			i += val.get();
//   	    		}
   	    		//Put put = new Put(Bytes.toBytes("some"));
   	    		//put.add("cooccurrence".getBytes(), "sdff".getBytes(), Bytes.toBytes(1));

   	    		//context.write(null, put);
   	 		
   	 		double cos_sim = 0;
   	 		for(DoubleWritable d : values) {
   	 			cos_sim += Double.parseDouble(d.toString());
   	 		}   	 	
   	 		String line = key.toString();
   	 		String [] str = line.split(",");
   	 		String w_u = str[0];
   	 		String w_v = str[1];
   	 		//if(cos_sim > theta) {
   	 			Put put1 = new Put(Bytes.toBytes(w_u));   	 		
   	 			put1.add("weight".getBytes(), w_v.getBytes(), Bytes.toBytes(cos_sim));
   	 			//System.out.println("GraphTable_cos_similatiry: key = " + w_u + " sec_key = " + w_v + " weight = " + cos_sim);
   	 			Put put2 = new Put(Bytes.toBytes(w_v));
   	 			put2.add("weight".getBytes(), w_u.getBytes(), Bytes.toBytes(cos_sim));
   	 			//System.out.println("GraphTable_cos_similatiry: key = " + w_v + " sec_key = " + w_u + " weight = " + cos_sim);
   	 			context.write(null, put1);
   	 			context.write(null, put2);
   	 		//}
   	   	}   	
	}
}
