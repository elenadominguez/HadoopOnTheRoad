


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestMultipleOutputMapper extends Mapper<Text, Text, Text, Text> {
	
	
	public void map(Text key, Text values, Context context) 
			throws IOException, InterruptedException{
		
			context.write(key, values);
	}
}


