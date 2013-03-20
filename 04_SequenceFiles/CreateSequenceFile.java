import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author elena
 * @see http://hadoopontheroad.blogspot.com
 * 
 * Clase que crea un SequenceFile en una ruta dada a partir de un texto est‡tico
 *
 */
public class CreateSequenceFile {

	private static final String[] POEMA = { 
		"El ciego sol se estrella",
		"en las duras aristas de las armas,",
		"llaga de luz los petos y espaldares",
		"y flamea en las puntas de las lanzas.",
		"El ciego sol, la sed y la fatiga",
		"Por la terrible estepa castellana,",
		"al destierro, con doce de los suyos",
		"-polvo, sudor y hierro- el Cid cabalga.",
		"Cerrado est‡ el mes—n a piedra y lodo.",
		"Nadie responde... Al pomo de la espada",
		"y al cuento de las picas el postigo",
		"va a ceder ÁQuema el sol, el aire abrasa!"};
	
	private static final String rutaDestino = new String ("pruebas/poemasequencefile");
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( conf);
		Path path = new Path(rutaDestino);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		//Creamos el writer del SequenceFile para poder ir a–adiendo
		// los pares key/value al fichero.
		SequenceFile.Writer writer = new SequenceFile.Writer(fs,  
				conf,  path, key.getClass(), value.getClass());
		
		for (int i = 0; i < POEMA.length; i++) { 
			// La key es el nœmero de l’nea
			key.set(i+1); 
			// El value es la l’nea del poema correspondiente
			value.set(POEMA[i]); 
			// Escribimos el par en el sequenceFile 
			writer.append(key, value);
		}
		
		writer.close();
	}
}
