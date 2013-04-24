import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Esta clase tiene que extender de la clase Mapper.
// Espera 4 tipos de datos: los 2 primeros definen 
// los tipos del key/value de entrada y los 2 œltimos 
// definen los tipos del key/value de salida.
public class WordCountMapper extends 
 	Mapper <LongWritable, Text, Text, IntWritable> {

	// Una buena pr‡ctica es la reutilizaci—n de objetos. 
	// Cuando necesitamos utilizar constantes, crear una 
	// variable est‡tica fuera del map.
	// De esta forma, cada vez que el mŽtodo map se llama,
	// no se crear‡ una nueva instancia de ese tipo.
	 private final static IntWritable cuenta = new IntWritable(1);
	 private Text palabra = new Text();
	
	// La funci—n que obligatoriamente tiene que 
	// implementarse en el Mapper es la map, que va
	// a recibir como par‡metros: primero el tipo de
	// la key, luego el tipo del value y finalmente un
	// objeto Context que se usar‡ para escribir los 
	// datos intermedios
	 public void map(LongWritable key, Text values, Context context) 
		   throws IOException, InterruptedException{
			// En el objeto "values" estamos recibiendo cada
			// l’nea del fichero que estamos leyendo. Primero
			// tenemos que pasarlo a String para poder 
			// operar con Žl
			  String linea = values.toString();
			  
			// Cada l’nea va a contener palabras separadas por
			// "un separador", separador que se considera como
			// una expresi—n regular y a partir del cual dividimos
			// la l’nea. Vamos recorriendo elemento a elemento. 
			  for(String word : linea.split(" ")){
				   if (word.length() > 0){
				//  Le damos el valor a nuestro objeto creado para la
				//  reutilizaci—n (claramente, a 'palabra', ya que 
				//  'cuenta' es una constante final static).
				//  Con el write escribimos los datos intermedios, que
				//  son como key la palabra y como valor un 1.
				    palabra.set(word);
				    context.write(palabra, cuenta);
				   }
			  }
	 }
}