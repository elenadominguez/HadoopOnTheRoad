


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TestMultipleOutputReducer extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text> multipleOut;
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOut.close();
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOut = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		String ruta = key.toString().replace("-", "");
		for(Text value:values){
			multipleOut.write( key, value, ruta);
		}
	
	}
}
