import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;


public class ReadMapFile {

	 private static final String rutaDestino = new String ("pruebas/poemamapfile4");
	 
	 public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
	  Configuration conf = new Configuration();
	  FileSystem fs = FileSystem.get(conf);

	  MapFile.Reader reader = new MapFile.Reader(fs, rutaDestino, conf);
	  
	  IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
	  Text value = (Text) reader.getValueClass().newInstance();
	  
	  StringBuilder strBuilder;
	  while(reader.next(key, value)){
	   strBuilder = new StringBuilder(key.toString()).append(" - ").append(value.toString());
	   System.out.println(strBuilder);
	  }

	  // Posiciono el reader en una key dada y conocida, si no existe la key dar’a un error
	  value = (Text)reader.get(new IntWritable(7), new Text());
	  strBuilder = new StringBuilder(" Probando get() ").append(value.toString());
	  System.out.println(strBuilder);
	  
	  // Busco una key a partir de un valor, que si no existe, me dar’a el valor
	  // siguiente m‡s pr—ximo y me posiciona el reader en ese lugar.
	  key = (IntWritable) reader.getClosest(new IntWritable(11), new Text());
	  strBuilder = new StringBuilder(" Probando getClosest() ").append(key.toString());
	  System.out.println(strBuilder);

	  
	  reader.close();
	 }
}

