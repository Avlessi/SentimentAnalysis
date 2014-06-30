package exper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.client.Delete;

public class RemoveEdgesDriver {
	
	public final static int TOP_N = 15;
	
	public static class RemoveEdgesMapper extends TableMapper<Text, Text> {	
		
		//Calculate a list L1 of TOP_N highest weighted edges adjacent to
		//wi and a list L! = L\L! where L – list of all edges adjacent to
	
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			String w = new String(row.get());
			System.out.println("key = " + w);
			Map<String, Double> map = new HashMap<String, Double>();
			
			for(KeyValue kv : value.raw()) {
	            //System.out.print("getRow = " + new String(kv.getRow()) + " " );
	            //System.out.print("getFamily = " + new String(kv.getFamily()) + ":" );
	            //System.out.print("qualifier = " + new String(kv.getQualifier()) + " " );           
	            double d = Bytes.toDouble(kv.getValue());
	            //System.out.println("value = " + d);	            
	            map.put(new String(kv.getQualifier()), d);
	        }
			
			
			ArrayList as = new ArrayList( map.entrySet() );  
	        
	        Collections.sort( as , new Comparator() {  
	            public int compare( Object o1 , Object o2 )  
	            {  
	                Map.Entry e1 = (Map.Entry)o1 ;  
	                Map.Entry e2 = (Map.Entry)o2 ;  
	                Double first = (Double)e1.getValue();  
	                Double second = (Double)e2.getValue();  
	                //return first.compareTo( second );
	                return second.compareTo( first );
	            }  
	        });
	        
	        //in 'as' array the values look like 'key=val'  
	        
	        
	        for(int i = TOP_N; i < as.size(); ++i) {
	        	String s = as.get(i).toString();
	        	//System.out.println("What about s? Here ----  " + s);
	        	s.lastIndexOf('=');
	        	String [] ar = new String [2];
	        	ar[0] = s.substring(0, s.lastIndexOf('='));
	        	ar[1] = s.substring(s.lastIndexOf('=') + 1, s.length());
	        	context.write(new Text(w), new Text(ar[0]));
	        	//System.out.println("RemoveEdgesMapper: key = " + w + " sec_key = " + ar[0]);
	        }
		}
	}	
	

    public static class RemoveEdgesReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	for(Text val : values) {
        		String str = val.toString();
        		
    			Delete delRow1 = new Delete(Bytes.toBytes(key.toString()));
    			delRow1.deleteColumn("weight".getBytes(), str.getBytes());
    			//System.out.println("RemoveEdgesReducer: key = " + key.toString() + " sec_key = " + str);
    			
    			Delete delRow2 = new Delete(Bytes.toBytes(str.toString()));
    			delRow2.deleteColumn("weight".getBytes(), key.getBytes());
    			//System.out.println("RemoveEdgesReducer: key = " + str + " sec_key = " + key.toString());
    			
    			context.write(null, delRow1);
    			context.write(null, delRow2);        		
        	}
        }
    }
    
}
