

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleDifferentOutsReducer extends Reducer<Text, PaperWritable, Text, Text> {
	
	private int contador;
	
	private MultipleOutputs<Text, Text> multipleOut;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		contador = 1;
		multipleOut = new MultipleOutputs<Text, Text>(context);
		
	}
	
	@Override
	public void reduce(Text key, Iterable<PaperWritable> values, Context context) 
				 throws IOException, InterruptedException {
		
		for (PaperWritable value : values) {
		
			//Output tabla Autor
			multipleOut.write("Autor", new IntWritable(contador), key);
			
			//Output tabla Paper
			multipleOut.write("Paper", value.getIdPaper(), value.getTituloPaper());

			//Output tabla paper/autor
			multipleOut.write("PaperAutor", value.getIdPaper(), new IntWritable(contador));
			
			contador ++;
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOut.close();

	}
}
