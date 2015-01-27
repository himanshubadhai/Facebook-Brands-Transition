package edu.uic.ids594;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * @author himansubadhai
 *
 */
public class PartitionerClass implements Partitioner<Text, Text>{

	public int getPartition(Text key, Text values, int numOfReduceTask) {
		// TODO Auto-generated method stub

		if(numOfReduceTask==0){
			return 0;
		}

		//parse the key to store in integer
		int partitionValue = Integer.parseInt(key.toString());

		// conditions of partition
		if(partitionValue>=1000 && partitionValue<1003)
		{
			return 0;
		}

		else if(partitionValue>=1004 && partitionValue<1007)
		{
			return 1%numOfReduceTask;
		}
		else
		{
			return 2%numOfReduceTask;
		}

	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
}