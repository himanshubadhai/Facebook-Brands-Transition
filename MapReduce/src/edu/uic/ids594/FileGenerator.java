package edu.uic.ids594;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * @author himansubadhai
 *
 */
public class FileGenerator {

	public static void main(String[] args)
	{
		Random random = new Random();

		//String Array to store brands
		String[] brandArray = {"brand0", "brand1", "brand2", "brand3", "brand4", "brand5", "brand6", "brand7", "brand8", "brand9"};
		// String[] timestampArray = {"ts0", "ts1", "ts2", "ts3", "ts4", "ts5", "ts6", "ts7", "ts8", "ts9"};
		File file = new File("input_mapReduce.txt");

		try
		{
			BufferedWriter output = new BufferedWriter(new FileWriter(file));

			for (int i = 0; i < 100; i++)
			{
				//generating random numbers between 1000 to 1009
				int userid = 1000 + random.nextInt(10);
				
				int brandid = random.nextInt(10);
				
				//getting brands assigned in brandArray
				String brand = brandArray[brandid];
				
				//generating random numbers for timestamp
				int timestamp = 100 + random.nextInt(888);

				output.write(userid + "\t" + brand + "\t" + timestamp);
				output.newLine();
			}
			output.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

