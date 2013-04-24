import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;


public class CreateMapFile {

	 private static final String[] POEMA = { 
	  "El ciego sol se estrella",
	  "en las duras aristas de las armas,",
	  "llaga de luz los petos y espaldares",
	  "y flamea en las puntas de las lanzas.",
	  "El ciego sol, la sed y la fatiga",
	  "Por la terrible estepa castellana,",
	  "al destierro, con doce de los suyos",
	  "-polvo, sudor y hierro- el Cid cabalga.",
	  "Cerrado está el mesón a piedra y lodo.",
	  "Nadie responde... Al pomo de la espada",
	  "y al cuento de las picas el postigo",
	  "va a ceder ¡Quema el sol, el aire abrasa!"};
	 
	 private static final String rutaDestino = new String ("pruebas/poemamapfile4");

	 public static void main(String[] args) throws IOException {
	  Configuration conf = new Configuration();
	  FileSystem fs = FileSystem.get( conf);
	  //Path path = new Path(rutaDestino);

	  IntWritable key = new IntWritable();
	  Text value = new Text();
	  
	  MapFile.Writer writer = new MapFile.Writer(conf, fs, 
	   rutaDestino, key.getClass(), value.getClass());
	  
	  //Indico un intervalo de índice diferente a 128 (por defecto)
	  writer.setIndexInterval(5);
	  
	  //No usaré posiciones consecutivas.
	  int pos = 1;
	  for (int i = 0; i < POEMA.length; i++) { 
	   // La key es el número de línea
	   key.set(pos); 
	   // El value es la línea del poema correspondiente
	   value.set(POEMA[i]); 
	   // Escribimos el par en el sequenceFile 
	   
	   writer.append(key, value);
	   pos += 3;
	  }
	  writer.close();
	 }

	}

