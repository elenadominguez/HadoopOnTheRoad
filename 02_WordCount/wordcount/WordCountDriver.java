package wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	public static void main(String[] args) throws Exception {
		
//		Se crea un nuevo Job indicando la clase que se llamará
//		al ejecutar y el nombre del Job.
//		Configuration servirá en programas más avanzados donde
//		queramos establecer configuraciones diferentes a las
//		que vienen por defecto o para el paso de parámetros.
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(WordCountDriver.class);
		
		job.setJobName("Word Count");
		
		
//		Indicamos cuáles son las clases Mapper y Reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(wordcount.WordCountReducer.class);
		//job.setNumReduceTasks(0);

//		Especificamos los directorios input y output, es decir, 
//      el directorio en HDFS donde se encuentra nuestro fichero 
//      de entrada, y dónde va a depositar los resultados
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//	FileInputFormat.setInputPaths(job, new Path("in/score.txt"));
	//	FileOutputFormat.setOutputPath(job, new Path("out5"));
		
//		Se establecen los tipos de la key y del value a la
//		salida del reduce.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		Se establecen los tipos de la key y del value a la
//		salida del map.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		Otras configuraciones posibles:
//		Por defecto el tipo del fichero de entrada es 
//		TextInputFormat, se puede cambiar con:
//		 job.setInputFormatClass(KeyValueTextInputFormat.class);
//		Por defecto la salida es un fichero de texto, 
//		se puede cambiar con:
//		 job.setOutputFormatClass(TextOutputFormat.class);
		 
//		Lanzamos el Job al cluster, hay varios modos, en 
//		waitForCompletion si hubiera más código implementado 
//		después de esta línea, no se ejecutaría
//		hasta que no finalizara el Job.
//		Hay otros modos.
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0:1);	
		

	}
}
