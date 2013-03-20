import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author elena
 *
 * Elimina el fichero 'nombreFichero' de HDFS
 * La ruta donde se encuentra hay que pasarla como argumento
 */
public class DeleteFileHdfs {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(args[0]+"/nombreFichero");
		hdfs.delete(path, false);
	}
}
