import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;




public class PersonaScoreReducer extends 
	Reducer<PersonaWritableComparable, IntWritable, 
	PersonaWritableComparable, IntWritable> {

	public void reduce(PersonaWritableComparable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int suma = 0;
		for (IntWritable value : values) {
			suma += value.get();
		}
		
		context.write(key, new IntWritable(suma));
	}
}
