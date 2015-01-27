package edu.uic.ids594;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author himansubadhai
 *
 */
public class Reducer1 extends MapReduceBase
implements Reducer<Text, Text, Text, Text>{

	private Text textVar = new Text();

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output1, Reporter reporter)
			throws IOException
	{
		String outputValue = "";

		// Storing the output value in a string separated by comma
		while (values.hasNext()) {
			outputValue = outputValue + ((Text)values.next()).toString() + ",";
		}

		// Storing the values in String Array
		String[] csvArray = outputValue.split(",");

		TreeMap<String, ArrayList<String>> sortedValues = new TreeMap<>();

		// for loop to sort the brand values based on timestamp
		for(String csv : csvArray){

			String keyTemp = csv.substring(0,2);
			String valueTemp = csv.substring(3);

			ArrayList<String> currentValue = new ArrayList<String>();

			if(sortedValues.containsKey(keyTemp)){
				currentValue= sortedValues.get(keyTemp);
				currentValue.add(valueTemp);
				sortedValues.put(keyTemp, currentValue);
			}
			else
			{
				currentValue.add(valueTemp);
				sortedValues.put(keyTemp, currentValue);
			}
		}

		// storing sorted brand values based on brand ID
		
		String str = sortedValues.values().toString();
		StringTokenizer tokens = new StringTokenizer(str,"[ ]");
		String tempStr = "";

		while(tokens.hasMoreTokens()){
			tempStr = tempStr + tokens.nextToken();
		}

		textVar.set(tempStr.trim());

		//long token = Integer.parseInt(key.toString());

		//LongWritable keyLong = new LongWritable(token);

		// collect the output in <Text, Text> format
		output1.collect(key, textVar);

		System.out.println("Key: " + key + " Sorted Values: " + textVar);
		//System.out.println("Done");

	} 
}
