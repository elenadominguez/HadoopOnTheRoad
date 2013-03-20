package wordcountoldapi;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class WordCountOldDriver {

	public static void main(String[] args) throws IOException {
		  
			  JobConf conf = new JobConf(WordCountOldDriver.class);
			  conf.setJobName("WorldCountOld");

			  conf.setMapperClass(WordCountOldMapper.class);
			  conf.setReducerClass(WordCountOldReducer.class);
			  
			  conf.setOutputKeyClass(Text.class);
			  conf.setOutputValueClass(IntWritable.class);
			  //conf.setMapOutputKeyClass(Text.class);
			  //conf.setMapOutputValueClass(IntWritable.class);

			  FileInputFormat.setInputPaths(conf, new Path(args[0]));
			  FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			  JobClient.runJob(conf);
			  
	}

}
