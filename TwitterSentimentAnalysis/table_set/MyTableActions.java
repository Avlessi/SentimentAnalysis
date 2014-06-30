package table_set;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import emoticons.SeedWordsHelper;

public class MyTableActions {
	
	public static void createPropagateTable(Configuration conf) {
		try{
			HBaseAdmin hba = new HBaseAdmin(conf);
			if(hba.tableExists("PropagateTable"))
				deleteAllRows(conf, "PropagateTable");
			hba.close();
			creatTable(conf, "PropagateTable", new String [] {"visited"});
		}
		catch(Exception e) {
			System.out.println("PropagateTable failed to create!!!!");
		}
	}
	
	public static void createGraphTable(Configuration conf) {
		try{
			HBaseAdmin hba = new HBaseAdmin(conf);
			if(hba.tableExists("GraphTable"))
				deleteAllRows(conf, "GraphTable");
			hba.close();
			creatTable(conf, "GraphTable", new String [] {"weight", "alpha"});
		}
		catch(Exception e) {
			System.out.println("GraphTable failed to create!!!!");
		}
	}
	
	public static void createCooccurrenceTable(Configuration conf) {
		try{
			HBaseAdmin hba = new HBaseAdmin(conf);
			if(hba.tableExists("CooccurrenceTable"))
				deleteAllRows(conf, "CooccurrenceTable");
			hba.close();
			creatTable(conf, "CooccurrenceTable", new String [] {"cooccurrence"});
		}
		catch(Exception e) {
			System.out.println("CooccurrenceTable failed to create!!!!");
		}
	}
	
	public static void createScoreTable(Configuration conf) {
		try{
			HBaseAdmin hba = new HBaseAdmin(conf);
			if(hba.tableExists("ScoreTable"))
				deleteAllRows(conf, "ScoreTable");
			hba.close();
			creatTable(conf, "ScoreTable", new String [] {"score"});
		}
		catch(Exception e) {
			System.out.println("ScoreTable failed to create!!!!");
		}
	}
	
	private static void creatTable(Configuration conf, String tableName, String [] families) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
            //System.out.println("delete its records");
            //disabletable(conf, tableName);
            //deletetable(conf, tableName);
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for(String family : families )
            	tableDesc.addFamily(new HColumnDescriptor(family));
            
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
        admin.close();
    }
	
	public static void deleteAllRows(Configuration conf, String tableName) throws IOException {
		HTable htable = new HTable(conf, tableName);
	    ResultScanner scanner = htable.getScanner(new Scan());
	    for(Result result: scanner) {
	        Delete delete = new Delete(result.getRow());
	        htable.delete(delete);
	    }
	    htable.close();
	}
	
	
	public static void disabletable(Configuration conf, String tablename) throws IOException{	    
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    admin.disableTable(tablename);
	    admin.close();
	}
	
	public static void deletetable(Configuration conf, String tablename) throws IOException{	    
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    admin.deleteTable(tablename);
	    admin.close();
	}
	

//    Put into the propagate table rows <wseed_i, <<visited: wseed_i>,
//    ‘’>>, where i = 1, 2, …, k and k is the number of seed nodes.
//    b) Create a column family named “alpha” in the graph table and
//    insert values < wi, <alpha: wi, 1>>
	public static void fillPropagateTableAndGraphTableWithSeedWords(Configuration conf) {		
		HTable propagateTable = null;
		HTable graphTable = null;
		try {
			propagateTable = new HTable(conf, "PropagateTable");
			graphTable = new HTable(conf, "GraphTable");
		} catch (IOException e) {			
			e.printStackTrace();
			System.out.println("Exception in creating Htable object for PropagateTable or GraphTable");
			return;
		}
		String propagateFamily = "visited";
		String graphFamily = "alpha";
		//insert emoticons to tables
		Set<String> set = SeedWordsHelper.getSeedSet();
		for(String seed : set){
			//insert to propagateTable
            Put propagate_put = new Put(Bytes.toBytes(seed));
            propagate_put.add(Bytes.toBytes(propagateFamily), Bytes.toBytes(seed),
            		Bytes.toBytes(""));
            try {
				propagateTable.put(propagate_put);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Exception while putting data in propagateTable");
			}
            //insert to graphTable
            Put graph_put = new Put(Bytes.toBytes(seed));
            graph_put.add(Bytes.toBytes(graphFamily), Bytes.toBytes(seed),
            		Bytes.toBytes(SeedWordsHelper.isPositive(seed) ? 1.0 : -1.0));
            try {
				graphTable.put(graph_put);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Exception while putting data in graphTable");
			}
		}	
		try {
			propagateTable.close();
			graphTable.close();
		} catch (IOException e) {			
			e.printStackTrace();
			System.out.println("Exception while closing of propagateTable or graphTable resources");
		}			
	}	
}