package edu.uic.ids594;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author himansubadhai
 *
 */
public class Reducer2 extends MapReduceBase implements Reducer<Text, Text, Text, LongWritable> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, LongWritable> output2, Reporter reporter)
			throws IOException
	
	{
		int sum = 0;
		
		// loop to calculate the count of each unique pair
		while (values.hasNext()) {
			values.next();
			sum++;
		}
		//System.out.println("Reducer: key- " + key + " value-" + sum);
		System.out.println("("+ key +")" + "\t" + sum);
		output2.collect(key, new LongWritable(sum));
	}
}
