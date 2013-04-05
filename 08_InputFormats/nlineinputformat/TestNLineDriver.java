package nlineinputformat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestNLineDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(TestNLineDriver.class);
		
		job.setJobName("Test NLineInputFormat");
		
		job.setMapperClass(TestNLineMapper.class);
		job.setReducerClass(TestNLineReducer.class);
		
		//Definimos el tipo del InputFormat
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setNumLinesPerSplit(job, 3);

		FileInputFormat.setInputPaths(job, new Path("pruebas/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("outnline1"));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
	}
}
