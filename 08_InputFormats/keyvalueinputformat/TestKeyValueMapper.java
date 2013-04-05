package keyvalueinputformat;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestKeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	private final static IntWritable cuenta = new IntWritable(1);
	
	public void map(Text key, Text values, Context context) 
			throws IOException, InterruptedException{
		
			context.write(key, cuenta);
	}
}


