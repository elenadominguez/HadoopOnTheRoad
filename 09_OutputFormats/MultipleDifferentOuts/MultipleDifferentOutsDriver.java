

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Hello world!
 *
 */
public class MultipleDifferentOutsDriver 
{
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
    	
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(MultipleDifferentOutsDriver.class);
		
		job.setJobName("WebIntelillence HW3");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PaperWritable.class);

		MultipleOutputs.addNamedOutput(job, "Autor",  TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Paper",  TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "PaperAutor",  TextOutputFormat.class, Text.class, IntWritable.class);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileInputFormat.setInputPaths(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapperClass(MultipleDifferentOutsMapper.class);
		job.setReducerClass(MultipleDifferentOutsReducer.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);		
    	
    }
}
