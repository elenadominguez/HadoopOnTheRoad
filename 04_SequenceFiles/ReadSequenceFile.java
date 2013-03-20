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
 *
 * Clase que lee un SequenceFile de una ruta dada y lo muestra por consola.
 * La aplicación busca puntos de sincronización (syncSeen) y cambia la posición
 * del reader a una posición conocida (seek) 
 */
public class ReadSequenceFile {

	private static final String rutaOrigen = new String ("pruebas/poemasequencefile");
	
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( conf);
		Path path = new Path(rutaOrigen);
		
		//Creamos el Reader del SequenceFile
		SequenceFile.Reader reader = 
				new SequenceFile.Reader(fs, path, conf);
		// Leemos la key y value del SequenceFile, los tipos son conocidos,
		// por lo que se declaran variables de esos tipos.
		IntWritable key = 
				(IntWritable) reader.getKeyClass().newInstance();
		Text value = 
				(Text) reader.getValueClass().newInstance();
		
		StringBuilder strBuilder;
		boolean haySync = false;
		long posSync = 0;
		
		//Recorremos el reader recuperando los pares key/value
		while(reader.next(key,value)){
			
			// Comprobamos si la posición es un punto de sync
			// En principio en este fichero no encontrará ninguno ya que es muy
			// pequeño, si fuera uno más grande y tuviera varios puntos de sync
			// se guardará el último punto encontrado.
			if(reader.syncSeen()){
				haySync = true;
				posSync = reader.getPosition();
			}
			
			strBuilder = new StringBuilder("Posición: ").
					append(reader.getPosition()).append(" - Key: ").
					append(key.toString()).append(" Value: " ).
					append(value.toString());
			System.out.println(strBuilder);
		}
		
		if(haySync){
			// reader.sync posicionará el reader en el sync siguiente más próximo, en
			// si no hay ninguno se posicionará al final del fichero.
			// En este caso se posicionará en el punto dado, ya que es de sync.
			strBuilder = new StringBuilder("Sync en el punto: ").
					append(posSync);
			System.out.println(strBuilder);
			reader.sync(posSync);
		}else{
			// Es un valor conocido, si no existiera, habría un error
			// al realizar el reader.next.
			posSync = 459;
			reader.seek(posSync);
		}
		
		// En un caso o en otro a pesar de haber finalizado la iteración hemos posicionado
		// el reader en un punto intermedio, así que seguimos recorriéndolo (repetimos las líneas)
		// hasta finalizar de nuevo.
		strBuilder = new StringBuilder("Volvemos a la posición: ").append(posSync);
		System.out.println(strBuilder);
		
		System.out.println("Y seguimos recorriendo el reader desde la posición dada: ");
		while(reader.next(key,value)){
			strBuilder = new StringBuilder("Posición: ").
					append(reader.getPosition()).append(" - Key: ").
					append(key.toString()).append(" Value: " ).
					append(value.toString());
			System.out.println(strBuilder);
		}
		
		reader.close();
	}

}
