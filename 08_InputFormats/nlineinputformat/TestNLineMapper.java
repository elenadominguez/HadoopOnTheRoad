package nlineinputformat;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TestNLineMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private final static IntWritable cuenta = new IntWritable(0);
	private Text texto = new Text();
	private StringBuilder pruebalinea = new StringBuilder();
	private int contador = 1;
	
	public void map(LongWritable key, Text values, Context context) 
			throws IOException, InterruptedException{
		String linea = values.toString();
		pruebalinea.append(values.toString()+ (contador < 3 ? " + " : ""));
		
		for(String word : linea.split("	")){
			if (word.length() > 0){
				if(contador == 3){
					texto.set(pruebalinea.toString());
					context.write(cuenta, texto);
					contador =0;
				}
			}
		}
		if(contador < 3) contador ++;
	}
}
