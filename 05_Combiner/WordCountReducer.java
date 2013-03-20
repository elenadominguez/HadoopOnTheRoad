import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends 
	Reducer<Text, IntWritable, Text, IntWritable> {
// En el reducer, al igual que en el mapper se podrían
// reutilizar los objetos declarándolos aquí. 
// Pero esta vez lo implemento sin usarlo para que 
// podáis ver cómo quedaría.


// El método reduce recibe 3 atributos, el primero
// es la key de entrada y el segundo es una lista
// de los valores intermedios asociados a esa key.
// Al igual que el Mapper, recibe el objeto Context
// para escribir la salida y otras informaciones.
 public void reduce(Text key, Iterable<IntWritable> values, 
		 Context context) 
				 throws IOException, InterruptedException {
  
	 	int count = 0;

		// Se va recorriendo la lista de valores y para cada
		// uno se extrae a través del .get() el valor correspondiente
		// Se van sumando esos valores para obtener el total
		// de veces que aparece una palabra.
		  for (IntWritable value : values) {
			  count += value.get();
		  }
		// Finalmente escribimos el resultado en HDFS usando 
		// el context.write
		  context.write(key, new IntWritable(count));
 }
}