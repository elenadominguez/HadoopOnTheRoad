import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author elena
 *
 * Crea un fichero en HDFS llamado 'nombreFichero'
 * La carpeta donde se quiere que se incluya hay
 * que pasarla como argumento
 */
public class CreateFileHdfs {

	public static void main(String[] args) throws Exception{
		String contentFile = "Este es el contenido del fichero\n";
		byte[] texto = contentFile.getBytes();
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(args[0]+"/nombreFichero");
		FSDataOutputStream outputStream = hdfs.create(path);
		outputStream.write(texto, 0, texto.length);
	
	}
}
