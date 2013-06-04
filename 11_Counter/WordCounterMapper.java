import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCounterMapper extends 
		Mapper <LongWritable, Text, Text, IntWritable> {

	private final static IntWritable cuenta = new IntWritable(1);
	private Text palabra = new Text();
	private String GRUPO_JUGADOR;
	private String GRUPO_ERROR;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		
		GRUPO_JUGADOR = conf.getStrings("grupos")[0];
		GRUPO_ERROR = conf.getStrings("grupos")[1];
	}

	@Override
	public void map(LongWritable key, Text values, Context context) 
		   throws IOException, InterruptedException{
		 
		
		 
		String linea = values.toString();
		String[] elems = linea.split("\t");
		  
		for(String word : elems){
			  if (word.length() > 0){
				  String player = "";
				  if(word.contains("Ana")){
					  player = "Ana";
				  }else if(word.contains("Pepe")){
					  player = "Pepe";
				  }else if(word.contains("Maria")){
					  player = "Maria";
				  }else if(word.contains("Pablo")){
					  player = "Pablo";
				  }
				  
				  if(!"".equals(player)){
					  context.getCounter(GRUPO_JUGADOR, player).increment(1);
					  if(elems.length < 3){
						  context.getCounter(GRUPO_ERROR, player).increment(1);
					  }
				  }
				  
				  palabra.set(word);
				  context.write(palabra, cuenta);
			   }
		}
	 }
}