package sentanalysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.swing.*;
import javax.swing.Box.Filler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.util.ArrayList;
import java.util.List;

import sentanalysis.SentimentAnalysis.SentimentAnalysisMapper;
import twitter4j.*;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;

public class SentimentAnalysisDriver {
	private static Configuration conf = HBaseConfiguration.create();
	private JTextArea txtArea = new JTextArea();
	
//	private static void createAndShowGUI() {
//        //Create and set up the window.
//        JFrame frame = new JFrame("SentimentAnalysis");
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        
//        //Display the window.
//        frame.pack();
//        frame.setVisible(true);
//        
//        SentimentAnalysisDriver sent = new SentimentAnalysisDriver();
//        frame.add(sent.getTextArea());   
//        
//    }
	
//	private JTextArea getTextArea() {
//		return txtArea;
//	}
	
	public static Configuration getHBaseConfiguration(){
		return conf; 
	}
	
	private void fillFileWithTweets() 
	{
		File file = new File("sentclassifierInput.txt");
		
		if(!file.exists())
		{
			try {
				file.createNewFile();
			} catch (IOException iox) {				
				System.out.println("failed to create file!");
				return;
			}
		}
		FileReader freader = null;		
		try{
			freader = new FileReader(file);
		}
		catch(FileNotFoundException ex){
			System.out.println("Exception with FileReader");
			return;
		}
		String quer = "lang:en";
		boolean bEmptyFile = true;
		//if file is not empty, return from function
		BufferedReader br = new BufferedReader(freader);     
		try {
			if (br.readLine() != null) {
				bEmptyFile = false;			    
			}
		} catch (IOException e2) {
			System.out.println("Exception while reading!");
			e2.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		if(!bEmptyFile)
			return;
		
		
	    ConfigurationBuilder cb = new ConfigurationBuilder();
				cb.setDebugEnabled(true)
				  .setOAuthConsumerKey("****************")
				  .setOAuthConsumerSecret("*******************************")
				  .setOAuthAccessToken("*****************************************")
				  .setOAuthAccessTokenSecret("***************************");
				
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();			
		
		try {
		    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("sentclassifierInput.txt", true)));		          
			
			Query query = new Query(quer);
		    query.setCount(10000);	    
		    
		    QueryResult result = null;
			try {
				result = twitter.search(query);
			} catch (TwitterException e1) {	
				e1.printStackTrace();
				System.out.println("twitter search exception!!");
			}

		    do{
	              List<Status> tweets = result.getTweets();

	              for(Status tweet: tweets){
	            	  out.println(tweet.getText().replaceAll("\n", " "));	            	  	                            	  
	              }

	              query=result.nextQuery();

	              if(query!=null)
					try {
						result=twitter.search(query);
					} catch (TwitterException e) {							
						e.printStackTrace();
						System.out.println("twitter search exception!!!");
					}

		    }while(query != null);
		    out.close();
		}
		catch(IOException e) {
			
		}

	}
	
	public void startAnalyser() throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = HBaseConfiguration.create();
		Job sentJob = new Job(conf, "SentimentJob");
		sentJob.setJarByClass(SentimentAnalysis.class);
    	FileInputFormat.addInputPath(sentJob, new Path("sentclassifierInput.txt"));
    	FileOutputFormat.setOutputPath(sentJob, new Path("SentClassifierOutput"));
    	sentJob.setMapperClass(SentimentAnalysisMapper.class);
    	sentJob.setMapOutputKeyClass(Text.class);
    	sentJob.setMapOutputValueClass(Text.class);
    	sentJob.setNumReduceTasks(0);
    	boolean b = sentJob.waitForCompletion(true);
    	if(!b)
    		throw new IOException("error with sentJob!");
	}
	
	public static void main(String[] args) {		
		
		SentimentAnalysisDriver sent = new SentimentAnalysisDriver();
		sent.fillFileWithTweets();
		try{
		sent.startAnalyser();
		}
		catch(IOException ex){
			System.out.println("IOException in sentiment job");
		}
		catch(ClassNotFoundException ex){
			System.out.println("ClassNotFoundException in sentiment job");
		}
		catch(InterruptedException ex){
			System.out.println("InterruptedException in sentiment job");
		}		
		
	    //Schedule a job for the event-dispatching thread:
	    //creating and showing this application's GUI.
//	    javax.swing.SwingUtilities.invokeLater(new Runnable() {
//         public void run() {
//	            createAndShowGUI();
//         }
//	    }); 
		
			
	        
	        
	    	        
	    }	
	
}
