import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends 
		Mapper <LongWritable, Text, Text, IntWritable> {

	private final static IntWritable cuenta = new IntWritable(1);
	private Text palabra = new Text();
	
	public void map(LongWritable key, Text values, Context context) 
		   throws IOException, InterruptedException{
		 
		Configuration conf = context.getConfiguration();
		
		String GRUPO_JUGADOR = conf.getStrings("grupos")[0];
		String GRUPO_ERROR = conf.getStrings("grupos")[1];
		 
		String linea = values.toString();
		String[] elems = linea.split("\t");
		  
		for(String word : elems){
			  if (word.length() > 0){
				  if(word.contains("Ana")){
					  context.getCounter(GRUPO_JUGADOR, "Ana").increment(1);
					  if(elems.length < 3)
						  context.getCounter(GRUPO_ERROR, "Ana").increment(1);
				  }else if(word.contains("Pepe")){
					  context.getCounter(GRUPO_JUGADOR, "Pepe").increment(1);
					  if(elems.length < 3)
						  context.getCounter(GRUPO_ERROR, "Pepe").increment(1);
				  }else if(word.contains("Maria")){
					  context.getCounter(GRUPO_JUGADOR, "Maria").increment(1);
					  if(elems.length < 3)
						  context.getCounter(GRUPO_ERROR, "Maria").increment(1);
				  }else if(word.contains("Pablo")){
					  context.getCounter(GRUPO_JUGADOR, "Pablo").increment(1);
					  if(elems.length < 3)
						  context.getCounter(GRUPO_ERROR, "Pablo").increment(1);
				  }
				  palabra.set(word);
				  context.write(palabra, cuenta);
			   }
		}
	 }
}