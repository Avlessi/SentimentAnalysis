package exper;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class WriteIndicesSetDriver {

	public static class WriteIndicesSetMapper extends TableMapper<Text, Text> {
		@Override
		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			  Text resText = null;
			  String prim_key = new String(row.get());
			  for(KeyValue kv : value.raw()) {
				  try{
					  double norm = (double)Integer.parseInt(new String(kv.getValue())) / prim_key.length();
					  //double norm = (double)Integer.parseInt(new String(kv.getValue()));
					  //double norm = (double)Integer.parseInt(new String(kv.getValue())) / kv.getQualifier().toString().length();
					  resText = new Text(prim_key + "," + String.valueOf(norm));	  
					  String qual = new String (kv.getQualifier());
					  context.write(new Text(qual), resText);
					  //System.out.println("WriteIndicesMapper: w_i = " + prim_key + " w_c = " + qual + " <w_c>, <w_i, norm_ic> = " + resText);
				  }
				  catch(Exception e) {
					  System.out.println("Exception in mapper for key = " + prim_key);
				  }
		  	  }
		  }		
	}
	
	public static class WriteIndicesSetReducer extends Reducer<Text, Text, Text, Text> {
		 @Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 StringBuilder sb = new StringBuilder();
			 for(Text str : values) {
				 sb.append(str.toString() + "///"); 
			 }
			 context.write(key, new Text(sb.toString()));			 
		 }
	}
	
	
	/*public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleRead");
		job.setJarByClass(WriteIndicesSetDriver.class);     // class that contains mapper

		Scan scan = new Scan();
        String columns = "cooccurrence"; // comma seperated        
        scan.addFamily(columns.getBytes());		
		

		TableMapReduceUtil.initTableMapperJob(
		  "CooccurenceTable",        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  MyMapper.class,   // mapper
		  Text.class,             // mapper output key
		  Text.class,             // mapper output value
		  job);
		//job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
		//job.setNumReduceTasks(0);
		job.setReducerClass(MyReducer.class);
		FileOutputFormat.setOutputPath(job, new Path("mySummaryFile"));
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
	}*/

}
