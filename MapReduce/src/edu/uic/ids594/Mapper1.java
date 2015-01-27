package edu.uic.ids594;

//import statements
import java.io.IOException;

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

/*
  LongWritable - Input key type
  Text - Input Value type
  Text - output key type
  Text - output value type
 */

public class Mapper1 extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>{

	// initialize variables
	private Text userid = new Text();
	private Text brandTimeStamp = new Text();

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output1, Reporter reporter) throws IOException
	{
		
		// read line by line and split them by tab
		String lineReader = value.toString();
		String[] tokenizer = lineReader.split("\\t");

		//setting output key
		this.userid.set(tokenizer[0]);
		
		//setting output value
		this.brandTimeStamp.set(tokenizer[2] + tokenizer[1]);
		
		//collecting output variables
		output1.collect(this.userid, this.brandTimeStamp);
		System.out.println(this.userid + "  " + this.brandTimeStamp);

	}
}
