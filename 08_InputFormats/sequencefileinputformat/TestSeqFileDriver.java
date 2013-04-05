package sequencefileinputformat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestSeqFileDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(TestSeqFileDriver.class);
		
		job.setJobName("Test SequenceFileInputFormat");
		
		job.setMapperClass(TestSeqFileMapper.class);
		job.setNumReduceTasks(0);
		
		//Definimos el tipo del InputFormat
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("pruebas/poemasequencefile"));
		FileOutputFormat.setOutputPath(job, new Path("outseqfile"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
	}
}
