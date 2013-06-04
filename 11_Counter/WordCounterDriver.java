


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounterDriver extends Configured implements Tool{
	
	String GRUPO_JUGADOR = "jugadorGroup";
	String GRUPO_ERROR = "errorJuegoGroup";
	  
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setStrings("grupos", GRUPO_JUGADOR, GRUPO_ERROR);
		
		Job job = new Job(conf);
		job.setJarByClass(WordCounterDriver.class);
		
		job.setJobName("Word Count");
		
		job.setMapperClass(WordCounterMapper.class);
		job.setReducerClass(WordCounterReducer.class);

		FileInputFormat.setInputPaths(job, new Path("pruebas/score.txt"));
		FileOutputFormat.setOutputPath(job, new Path("pruebas/out5"));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		
		long tipoAna = job.getCounters().findCounter(GRUPO_JUGADOR, "Ana").getValue();
		long tipoPepe = job.getCounters().findCounter(GRUPO_JUGADOR, "Pepe").getValue();
		long tipoMaria = job.getCounters().findCounter(GRUPO_JUGADOR, "Maria").getValue();
		long tipoPablo = job.getCounters().findCounter(GRUPO_JUGADOR, "Pablo").getValue();
		
		long tipoErrorAna = job.getCounters().findCounter(GRUPO_ERROR, "Ana").getValue();
		long tipoErrorPepe = job.getCounters().findCounter(GRUPO_ERROR, "Pepe").getValue();
		long tipoErrorMaria = job.getCounters().findCounter(GRUPO_ERROR, "Maria").getValue();
		long tipoErrorPablo = job.getCounters().findCounter(GRUPO_ERROR, "Pablo").getValue();
		
		System.out.println("Ana:   "+tipoAna+" - Errores: "+tipoErrorAna);
		System.out.println("Pepe:  "+tipoPepe+" - Errores: "+tipoErrorPepe);
		System.out.println("Maria: "+tipoMaria+" - Errores: "+tipoErrorMaria);
		System.out.println("Pablo: "+tipoPablo+" - Errores: "+tipoErrorPablo);
		
		return (success ? 0:1);	

	}
		
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new WordCounterDriver(), args);
	    System.exit(exitCode);
	}
		
}
