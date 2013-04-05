package multipleinputformat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMultipleDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(TestMultipleDriver.class);
		
		job.setJobName("Word Count");
		
		job.setMapperClass(TestKeyValueMapper.class);
		job.setReducerClass(TestMultipleReducer.class);
		
		//Definimos el tipo del InputFormat

		MultipleInputs.addInputPath(job, new Path("pruebas/score.txt"), KeyValueTextInputFormat.class, TestKeyValueMapper.class);
		MultipleInputs.addInputPath(job, new Path("pruebas/poemasequencefile"), SequenceFileInputFormat.class, TestSeqFileMapper.class);
		
		FileInputFormat.setInputPaths(job, new Path("pruebas/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("outkeyvalue2"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
	}
}
