import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PersonaScoreDriver {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("Ha ocurrido un error en la entrada");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(PersonaScoreDriver.class);
		
		job.setJobName("Persona Score");
		
		job.setOutputKeyClass(PersonaWritableComparable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(PersonaScoreMapper.class);
		job.setReducerClass(PersonaScoreReducer.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);		
	}
}
