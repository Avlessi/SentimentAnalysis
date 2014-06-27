package exper;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import auxiliary.BetaCalculator;
import auxiliary.UpdateScore;

import table_set.MyTableActions;

import exper.CosineSimilarityDriver.CosineSimilarityMapper;

import exper.CalculateBetaDriver.CalculateBetaMapper;
import exper.CalculateBetaDriver.CalculateBetaReducer;
import exper.CooccurrenceDriver.CooccurrenceMapper;
import exper.CooccurrenceDriver.CooccurrenceReducer;
import exper.CosineSimilarityDriver.CosineSimilarityReducer;
import exper.PosNegScoreCalculationDriver.PosNegScoreCalculationMapper;
import exper.PosNegScoreCalculationDriver.PosNegScoreCalculationReducer;
import exper.PropagateScoreDriver.PropagateScoreMapper;
import exper.PropagateScoreDriver.PropagateScoreReducer;
import exper.RemoveEdgesDriver.RemoveEdgesMapper;
import exper.RemoveEdgesDriver.RemoveEdgesReducer;
import exper.WriteIndicesSetDriver.WriteIndicesSetMapper;
import exper.WriteIndicesSetDriver.WriteIndicesSetReducer;


public class GeneralDriver {
	
	private static Configuration hbase_conf = null;
	private static double beta = 0;
	private static int distance = 4;
	private static int window_size = 6;
	
	public static void main(String[] args) throws Exception {
        
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("look.txt", true)));
		
		
    	Configuration conf = HBaseConfiguration.create();    	
	     
	    MyTableActions.createCooccurrenceTable(conf);
	    MyTableActions.createGraphTable(conf);
	    MyTableActions.createPropagateTable(conf);
	    MyTableActions.fillPropagateTableAndGraphTableWithSeedWords(conf);     	
    	MyTableActions.createScoreTable(conf);
    	
    	Job cooccurrenceJob = new Job(conf, "CooccurrenceJob");
    	cooccurrenceJob.setJarByClass(CooccurrenceDriver.class);
    	//FileInputFormat.addInputPath(cooccurrenceJob, new Path("littlexample.txt"));
    	FileInputFormat.addInputPath(cooccurrenceJob, new Path("taggerOutput.txt"));
    	cooccurrenceJob.setMapperClass(CooccurrenceMapper.class);
    	cooccurrenceJob.setMapOutputKeyClass(Text.class);
    	cooccurrenceJob.setMapOutputValueClass(MapWritable.class);
    	TableMapReduceUtil.initTableReducerJob("CooccurrenceTable", CooccurrenceReducer.class, cooccurrenceJob);
    	boolean b = cooccurrenceJob.waitForCompletion(true);
    	if(!b)
    		throw new IOException("error with CooccurrenceJob!");
    	
    	pw.println("CooccurenceJob");
    	
        Job writeIndecesJob = new Job(conf, "WriteIndeces");
        writeIndecesJob.setJarByClass(WriteIndicesSetDriver.class);
        Scan writeIndecesScan = new Scan();
        String columns = "cooccurrence"; // comma seperated        
        writeIndecesScan.addFamily(columns.getBytes());		

		TableMapReduceUtil.initTableMapperJob(
		  "CooccurrenceTable",        // input HBase table name
		  writeIndecesScan,             // Scan instance to control CF and attribute selection
		  WriteIndicesSetMapper.class,   // mapper
		  Text.class,             // mapper output key
		  Text.class,             // mapper output value
		  writeIndecesJob);
		//job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
		//job.setNumReduceTasks(0);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		writeIndecesJob.setReducerClass(WriteIndicesSetReducer.class);
		FileOutputFormat.setOutputPath(writeIndecesJob, new Path("mySummaryFile"));
		b = writeIndecesJob.waitForCompletion(true);
		if (!b) 
		  throw new IOException("error with WriteIndecesJob!");
		
		pw.println("WriteIndecesJob");
		////////////////////
		Job cosSimJob = new Job(conf, "CosineSimilarityJob");
		cosSimJob.setJarByClass(CosineSimilarityDriver.class);		
        
        FileInputFormat.addInputPath(cosSimJob, new Path("mySummaryFile/part-r-00000"));
        
        cosSimJob.setMapperClass(CosineSimilarityMapper.class);
        
        cosSimJob.setMapOutputKeyClass(Text.class);
        cosSimJob.setMapOutputValueClass(DoubleWritable.class);
        cosSimJob.setOutputKeyClass(Text.class);
    	cosSimJob.setOutputValueClass(DoubleWritable.class);
    	
        TableMapReduceUtil.initTableReducerJob("GraphTable", CosineSimilarityReducer.class, cosSimJob);
        
        b = cosSimJob.waitForCompletion(true);
		if (!b) 
		  throw new IOException("error with CosineSimilarityJob!");
		
		System.out.println("CosineSimilarityJob executed!!!!!!!!!!!!!!!!!");
		pw.println("CosineSimilarityJob");
		pw.close();
		/////////////////////////////
		Job removeEdgesJob = new Job(conf, "RemoveEdgesJob");
		removeEdgesJob.setJarByClass(RemoveEdgesDriver.class);
		
		Scan remEdgesScan = new Scan();
		remEdgesScan.setCaching(500);
	    String remEdgesColumns = "weight"; // comma seperated        
	    remEdgesScan.addFamily(remEdgesColumns.getBytes()); 
	       
	    TableMapReduceUtil.initTableMapperJob("GraphTable", remEdgesScan, RemoveEdgesMapper.class, Text.class, Text.class, removeEdgesJob);	     
	    TableMapReduceUtil.initTableReducerJob("GraphTable", RemoveEdgesReducer.class, removeEdgesJob);
	    b = removeEdgesJob.waitForCompletion(true);
	    if(!b)
	    	throw new IOException("error with RemoveEdgesJob!");
	    
	    
	    System.out.println("RemoveEdgesJob executed!!!!!!!!!!!!!!!!!");
	    pw.println("RemoveEdgesJob executed!!!!!!!!!!!!!!!!!"); 
	     
	     /////////////////////////////
    	
    	//MyTableActions.createPropagateTable(conf);
	    //MyTableActions.fillPropagateTableAndGraphTableWithSeedWords(conf);
    	
	    for(int i = 0; i < 2; ++i){
		    Job propagateScoreJob = new Job(conf, "PropagateScoreJob");
		    propagateScoreJob.setJarByClass(PropagateScoreDriver.class);
		    Scan propagateScoreScan = new Scan();
		    propagateScoreScan.setCaching(500);
		    propagateScoreScan.addFamily(Bytes.toBytes("visited"));
		    TableMapReduceUtil.initTableMapperJob("PropagateTable", propagateScoreScan, PropagateScoreMapper.class, Text.class, Text.class, propagateScoreJob);
		    TableMapReduceUtil.initTableReducerJob("PropagateTable", PropagateScoreReducer.class, propagateScoreJob); 
		    b = propagateScoreJob.waitForCompletion(true);
		    if(!b)
		    	throw new IOException("error with PropagateScoreJob");
	    }
		System.out.println("PropagateScore executed!!!!");
	    /////////////////////////////	    
	    Job posnegScoreCalculationJob = new Job(conf, "PosNegScoreCalculationJob");
	    posnegScoreCalculationJob.setJarByClass(PosNegScoreCalculationDriver.class);
	    Scan posnegScoreCalculationScan = new Scan();
	    posnegScoreCalculationScan.setCaching(500);
	    posnegScoreCalculationScan.addFamily(Bytes.toBytes("alpha"));
	    TableMapReduceUtil.initTableMapperJob("GraphTable", posnegScoreCalculationScan, PosNegScoreCalculationMapper.class, Text.class, Text.class, posnegScoreCalculationJob);
	    posnegScoreCalculationJob.setReducerClass(PosNegScoreCalculationReducer.class);
	    posnegScoreCalculationJob.setOutputKeyClass(Text.class);
	    posnegScoreCalculationJob.setOutputValueClass(Text.class);
	    FileOutputFormat.setOutputPath(posnegScoreCalculationJob, new Path("PosNegValuesBeforeBeta"));
	    b = posnegScoreCalculationJob.waitForCompletion(true);
	    
	    if(!b)
	    	throw new IOException("error with PosNegScoreCalculationJob");
	    else
	    	System.out.println("PosNegScoreCalculationJob executed");
    
	    
	    ///////////////////////////
	    /*Job calculateBetaJob = new Job(conf, "CalculateBetaJob");
	    calculateBetaJob.setJarByClass(CalculateBetaDriver.class);
	    //FileInputFormat.addInputPath(cosSimJob, new Path("PosNegValuesBeforeBeta/part-r-00000"));     !!!!!!!!!!!!
	    calculateBetaJob.setMapperClass(CalculateBetaMapper.class);
	    calculateBetaJob.setReducerClass(CalculateBetaReducer.class);
	    
	    calculateBetaJob.setMapOutputKeyClass(Text.class);
	    calculateBetaJob.setMapOutputValueClass(Text.class);
	    calculateBetaJob.setOutputKeyClass(Text.class);
	    calculateBetaJob.setOutputValueClass(Text.class);
	    b = calculateBetaJob.waitForCompletion(true);
	    if(!b)
	    	throw new IOException("error with CalculateBetaJOb");*/
		
		
	    
	    beta = BetaCalculator.calculate("PosNegValuesBeforeBeta/part-r-00000");
	    System.out.println("beta = " + beta);
	    try{
	    UpdateScore.update(conf, "PosNegValuesBeforeBeta/part-r-00000", beta);
	    }
	    catch(Exception e){
	    	System.out.println("Exception in update function!!!");
	    	e.printStackTrace();
	    }
    }
    
    public static Configuration getHBaseConf() {
		if(hbase_conf == null)
			hbase_conf = HBaseConfiguration.create();
		return hbase_conf;
	}
    
    public static double getBeta() {
    	return beta;
    }	
    
    public static int getWindowSize(){
    	return window_size;
    }
}
