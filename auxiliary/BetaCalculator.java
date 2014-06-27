package auxiliary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BetaCalculator {
	
	public static double calculate(String pathToFile){
		BufferedReader br = null;
		double beta = 0;
		double posValue = 0;
		double negValue = 0;
		try { 
			String sCurrentLine; 
			br = new BufferedReader(new FileReader(pathToFile));
			
			while ((sCurrentLine = br.readLine()) != null) {
				//System.out.println(sCurrentLine);
				String [] value = sCurrentLine.split("\t");
				String [] scores = value[1].split(" ");
				posValue += Double.parseDouble(scores[0]);
				negValue += Double.parseDouble(scores[1]);				
			}
 
		} catch (IOException e) {
			System.out.println("Exception!!!");
			e.printStackTrace();
		} finally {
			beta = 0;
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
				System.out.println("Exception!!!");
			}
		}
		
		System.out.println("posValue = " + posValue);
		System.out.println("negValue = " + negValue);
		beta = posValue / negValue;
		
		return beta;
	}
}