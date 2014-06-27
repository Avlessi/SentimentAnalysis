package auxiliary;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
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

public class UpdateScore {
	public static void update(Configuration conf, String pathToFile, double beta) throws Exception {
		HTable table = new HTable(conf, "ScoreTable");
		BufferedReader br = new BufferedReader(new FileReader(pathToFile));
		String curLine;
		while((curLine = br.readLine()) != null) {
			String [] words = curLine.split("\t");
			String key = words[0];
			String [] values = words[1].split(" ");
			double posScore = Double.valueOf(values[0]);
			double negScore = Double.valueOf(values[1]);
			double finalScore = posScore - beta * negScore;
			Put put = new Put(Bytes.toBytes(key));
			put.add(Bytes.toBytes("score"), Bytes.toBytes(key), Bytes.toBytes(finalScore));
			table.put(put);
		}
		br.close();		
		table.close();
	}
}
