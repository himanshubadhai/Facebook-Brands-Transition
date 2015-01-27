package edu.uic.ids594;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author himansubadhai
 *
 */
public class Mapper2 extends MapReduceBase
implements Mapper<LongWritable, Text, Text, LongWritable>{

	//private String brandPair;
	Text productPair = new Text();
	public static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output2, Reporter reporter)
			throws IOException
	{
		String line = value.toString();

		// splitting by tab to get values
		String[] keyValue= line.split("\t");
		String values = keyValue[1];

		// StringTokenizer tokens = new StringTokenizer(values,",");
		
		// split the values by comma to get brands
		String brandValues[]=values.split(",");

		Text text = new Text();

		LongWritable one = new LongWritable(1); 

		//while(tokens.hasMoreTokens()){
		//tokens.nextToken();
		//String brandsToken = tokens.nextToken().toString();

		//String[] tokenizedData = values.toString().split(",");


		// for loop to generate brand pairs
		for(int i=0; i< brandValues.length; i++){

			if(brandValues.length == 1){
				text.set(brandValues[i]);
				output2.collect(text, one);
			}

			else{

				for (int j=i+1; j< brandValues.length; j++){
					text.set(brandValues[i] + "," + brandValues[j]);					
					System.out.println("(" + brandValues[i] + "," + brandValues[j] + ")");
					output2.collect(text, one);
				}
			}

		}
		//}
	}
}
