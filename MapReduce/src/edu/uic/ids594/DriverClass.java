package edu.uic.ids594;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author himansubadhai
 *
 */
public class DriverClass extends Configured
implements Tool{

	public int run(String[] args)
			throws Exception {

		// creating configuration object for job-1
		JobConf config1 = new JobConf(getConf(), DriverClass.class);
		config1.setJobName("MapReduceJob1");

		//setting configuration parameters for job-1
		config1.setOutputKeyClass(Text.class);
		config1.setOutputValueClass(Text.class);
		config1.setMapOutputKeyClass(Text.class);
		config1.setMapOutputValueClass(Text.class);

		// setting classes for job-1
		config1.setJarByClass(DriverClass.class);
		config1.setMapperClass(Mapper1.class);
		config1.setReducerClass(Reducer1.class);
		config1.setPartitionerClass(PartitionerClass.class);
		config1.setNumReduceTasks(3);

		//setting input and output file path for job-1
		FileInputFormat.addInputPath(config1, new Path("/Users/himansubadhai/Documents/input"));
		FileOutputFormat.setOutputPath(config1, new Path("/Users/himansubadhai/Documents/input/output_intermediate"));
		JobClient.runJob(config1);

		// creating configuration object for job-2
		JobConf config2 = new JobConf(getConf(), DriverClass.class);
		config2.setJobName("MapReduceJob2");

		//setting configuration parameters for job-2
		config2.setOutputKeyClass(Text.class);
		config2.setOutputValueClass(LongWritable.class);
		config2.setMapOutputKeyClass(Text.class);
		config2.setMapOutputValueClass(LongWritable.class);

		// setting classes for job-2		
		config2.setJarByClass(DriverClass.class);
		config2.setMapperClass(Mapper2.class);
		config2.setReducerClass(Reducer2.class);

		//setting input and output file path for job-1
		FileInputFormat.addInputPath(config2, new Path("/Users/himansubadhai/Documents/input/output_intermediate"));
		FileOutputFormat.setOutputPath(config2, new Path("/Users/himansubadhai/Documents/output"));
		JobClient.runJob(config2);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DriverClass(), args);
		System.exit(res);
	}
}
