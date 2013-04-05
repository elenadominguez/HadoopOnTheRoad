package multipleinputformat;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestSeqFileMapper extends Mapper<IntWritable, Text, Text, Text> {
	
	
	public void map(IntWritable key, Text values, Context context) 
			throws IOException, InterruptedException{
		
		context.write(new Text(key.toString()), values);
	}
}


