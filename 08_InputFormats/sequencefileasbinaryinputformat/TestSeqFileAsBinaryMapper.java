package sequencefileasbinaryinputformat;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestSeqFileAsBinaryMapper extends Mapper<BytesWritable, BytesWritable, IntWritable, Text> {
	
	public void map(BytesWritable key, BytesWritable values, Context context) 
			throws IOException, InterruptedException{
		
		byte[] bkey = key.getBytes();
		byte[] bvalue = values.getBytes();
		
		System.out.println(new String(bkey));
		System.out.println(new String(bvalue));
		
		ByteBuffer buffer = ByteBuffer.wrap(bkey);
		
		context.write(new IntWritable(buffer.getShort()),
				new Text(new String(values.getBytes(), "ISO-8859-1")));
	}
	
	
}


