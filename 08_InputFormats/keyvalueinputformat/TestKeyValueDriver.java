package keyvalueinputformat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestKeyValueDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(TestKeyValueDriver.class);
		
		job.setJobName("Word Count");
		
		job.setMapperClass(TestKeyValueMapper.class);
		job.setReducerClass(TestKeyValueReducer.class);
		
		//Definimos el tipo del InputFormat
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("pruebas/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("outkeyvalue"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
	}
}
