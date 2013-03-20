import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PersonaScoreMapper extends 
	Mapper<LongWritable, Text, 
	PersonaWritableComparable, IntWritable> {

	private IntWritable score = new IntWritable();
	PersonaWritableComparable persona = new PersonaWritableComparable();
	
	public void map(LongWritable key, Text values,
			Context context) throws IOException, InterruptedException {
		
		// El texto tiene este formato:
		// 01-11-2012	Maria Garcia Martinez 11
		// La fecha separada por tabulaci—n, el resto con espacios
		String[] primerSplit = values.toString().split("	");
		if(primerSplit.length == 2){
			String[] segundoSplit = primerSplit[1].split(" ");
			
			// Puede haber personas con un apellido o con dos
			if(segundoSplit.length == 3 || segundoSplit.length == 4){
				if(segundoSplit.length == 3){
					persona.set(segundoSplit[0], segundoSplit[1], "");
					score.set(Integer.valueOf(segundoSplit[2]));
				}else {
					persona.set(segundoSplit[0], segundoSplit[1], segundoSplit[2]);
					score.set(Integer.valueOf(segundoSplit[3]));
				}
				context.write(persona, score);
			}	
		}
	}
}
