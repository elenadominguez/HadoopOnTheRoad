package erroneo;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author elena
 * Recuerda que este es un ejemplo de lo que NO SE DEBE HACER
 */
public class PersonaScoreDriver {
	
	private static int score;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//Indicamos el par‡metro a pasar
		
		score = 25;
				
		Job job = new Job(conf);
		job.setJarByClass(PersonaScoreDriver.class);
		job.setJobName("Persona Score");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);		
	}
	
	private static class MyMapper extends Mapper<Text, Text, Text, Text>{
		@Override
		public void map(Text key, Text value,
				Context context) throws IOException, InterruptedException {
			
			int s = score;
			String[] personaSplit = value.toString().split(" ");
			
			if(personaSplit.length == 4){
				int mScore = Integer.valueOf(personaSplit[3]);
				
				if(mScore >= s)
					context.write(key, value);
			}
		}
	}
}
