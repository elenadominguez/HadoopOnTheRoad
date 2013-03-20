import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author elena
 *
 * Lee el contenido del fichero de HDFS 'nombreFichero'
 * La ruta donde se encuentra hay que pasarla como argumento
 */
public class ReadFileHdfs {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(args[0]+"/nombreFichero");
		FSDataInputStream inputStream = hdfs.open(path);
		IOUtils.copyBytes(inputStream, System.out, conf, true);	
	}
}
