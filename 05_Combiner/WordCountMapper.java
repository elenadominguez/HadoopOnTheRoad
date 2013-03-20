import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends 
 	Mapper <LongWritable, Text, Text, IntWritable> {

	 private final static IntWritable cuenta = new IntWritable(1);
	 private Text palabra = new Text();
	
	 public void map(LongWritable key, Text values, Context context) 
		   throws IOException, InterruptedException{
			  String linea = values.toString();
			  
			  for(String word : linea.split(" ")){
				   if (word.length() > 0){
				    palabra.set(word);
				    context.write(palabra, cuenta);
				   }
			  }
	 }
}
