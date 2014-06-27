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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PropagateScoreDriver {
	
	
	public static class PropagateScoreMapper extends TableMapper<Text, Text> {
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			String w_seed = new String(row.get());
			
			//For each qualifier wiu of column family “visited” in row 	wseed_i,
			for(KeyValue kv : value.raw()) {
				String w_iu = new String(kv.getQualifier());
				//System.out.println("PropagateScoreMapper: key = " + w_seed + " qualifier = " + w_iu);
				
				//For each node w_iv which is adjacent to w_iu in the word graph and which is not in column family “visited” in row w_seed_i
				HTable table = new HTable(GeneralDriver.getHBaseConf(),"GraphTable");
				Get get = new Get(Bytes.toBytes(w_iu));
				get.addFamily(Bytes.toBytes("weight"));
				Result rs = table.get(get);
		        	
		        for(KeyValue keyval : rs.raw()) {
		            String w_iv = new String(keyval.getQualifier());
		            //System.out.println("w_iv node adjacent to w_iu in GraphTable is " + w_iv);
		            if(!presentInVisited(w_iv, value.raw())){
		            	//System.out.println("w_iv is not present in column family “visited” in row w_seed_i. Write to reducer.");
		            	context.write(new Text(w_seed), new Text(w_iu + "," + w_iv));
		            	System.out.println("w_seed = " + w_seed + "w_iu,w_iv = " + w_iu + "," + w_iv);
		            }
		            //else
		            	//System.out.println("w_iv is present in column family “visited” in row w_seed_i");
		        }				
				table.close();
			}
		}
		
		public boolean presentInVisited(String val, KeyValue [] kvSet) {
			for(KeyValue kv: kvSet){
				String word = new String(kv.getQualifier());
				if(val.equals(word)) 
					return true;
			}
			return false;
		}
	}
	
	public static class PropagateScoreReducer extends TableReducer<Text, Text, ImmutableBytesWritable>  {   		

   	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   	 		//Retrieve alpha_iu from entry < wseed_i, <alpha: wiu, alpha_iu>> in the
   	 		//graph table. If that entry does not exist, assign alpha_iu ≔ 0
   	 		String w_seed = key.toString();
   	 		//System.out.println("PropagateScoreReducer: key = " + w_seed);
   	 		HTable table = new HTable(GeneralDriver.getHBaseConf(),"GraphTable");
   	 		for(Text text: values) {
   	 			String [] line = text.toString().split(",");
   	 			String w_iu = line[0];
   	 			String w_iv = line[1];
   	 			//System.out.println("w_iu = " + w_iu);
   	 			//System.out.println("w_iv = " + w_iv);
   	 			double alpha_iu = 0;
   	 			double alpha_iv = 0;
		   	 	
				Get get = new Get(Bytes.toBytes(w_seed));
				get.addColumn(Bytes.toBytes("alpha"), Bytes.toBytes(w_iu));
				Result rs = table.get(get);
				if(!rs.isEmpty()){				
					alpha_iu = Bytes.toDouble(rs.getValue(Bytes.toBytes("alpha"), Bytes.toBytes(w_iu)));
					//System.out.println("alpha_iu = " + alpha_iu);
				}
				else {
					Put put = new Put(Bytes.toBytes(w_seed));
					put.add(Bytes.toBytes("alpha"), Bytes.toBytes(w_iu), Bytes.toBytes(0.0));
					table.put(put);
				}
				
				Get get2 = new Get(Bytes.toBytes(w_seed));
				get2.addColumn(Bytes.toBytes("alpha"), Bytes.toBytes(w_iv));
				Result rs2 = table.get(get2);
				if(!rs2.isEmpty()){
					alpha_iv = Bytes.toDouble(rs2.getValue(Bytes.toBytes("alpha"), Bytes.toBytes(w_iv)));
					//System.out.println("alpha_iv = " + alpha_iv);
				}
				else {
					Put put = new Put(Bytes.toBytes(w_seed));
					put.add(Bytes.toBytes("alpha"), Bytes.toBytes(w_iv), Bytes.toBytes(0.0));
					table.put(put);					
				}
				
				//Get the weight w_uv of edge (wiu, wiv) from the graph table.
				double w_uv = 0;
				Get weight_get = new Get(Bytes.toBytes(w_iu));
				weight_get.addColumn(Bytes.toBytes("weight"), Bytes.toBytes(w_iv));
				Result weight_res = table.get(weight_get);
				if(!weight_res.isEmpty())
				{
					w_uv = Bytes.toDouble(weight_res.getValue(Bytes.toBytes("weight"), Bytes.toBytes(w_iv)));
					//System.out.println("w_uv = " + w_uv);
				}
				
				if(alpha_iv < alpha_iu * w_uv){
					alpha_iv = alpha_iu * w_uv;
					//System.out.println("put in GraphTable to alpha family new alpha_iv which is " + alpha_iv);
					Put put = new Put(Bytes.toBytes(w_seed));
					put.add(Bytes.toBytes("alpha"), Bytes.toBytes(w_iv), Bytes.toBytes(alpha_iv));
					table.put(put);	
				}
				
				Put propagate_put = new Put(Bytes.toBytes(w_seed));
				//System.out.println("PropagateScoreReducer: w_seed is " + w_seed + " visited is = " + w_iv);
   	 			propagate_put.add("visited".getBytes(), Bytes.toBytes(w_iv), Bytes.toBytes(""));
				context.write(null, propagate_put);
   	 		}	
   	 		table.close();
   	 	}
	}		
}