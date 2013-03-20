

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *  @author elena
 *  
 * Pasa un fichero del sistema de archivos local
 * a la carpeta de usuario de HDFS.
 * Los nombres de fichero de origen y de destino
 * hay que pasarlos como argumentos
 */
public class LocalToHdfs {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path sourcePath = new Path(args[0]);
		Path destPath = new Path(args[1]);
		hdfs.copyFromLocalFile(sourcePath, destPath);
	}
}
