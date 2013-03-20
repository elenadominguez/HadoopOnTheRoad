import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PersonaScorePartitioner extends Partitioner<Text, Text> {
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		if(key.toString().endsWith("2012")){
			return 0;
		}else if(key.toString().endsWith("2013")){
			return 1;
		}else{ //Others
			return 2;
		}
	}
}
