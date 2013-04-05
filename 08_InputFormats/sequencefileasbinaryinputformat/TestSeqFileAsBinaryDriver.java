package sequencefileasbinaryinputformat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestSeqFileAsBinaryDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(TestSeqFileAsBinaryDriver.class);
		
		job.setJobName("Test SequenceFileAsBinaryInputFormat");
		
		job.setMapperClass(TestSeqFileAsBinaryMapper.class);
		job.setNumReduceTasks(0);
		
		//Definimos el tipo del InputFormat
		job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("pruebas/poemasequencefile"));
		FileOutputFormat.setOutputPath(job, new Path("outseqfilebinary7"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
	}
}
