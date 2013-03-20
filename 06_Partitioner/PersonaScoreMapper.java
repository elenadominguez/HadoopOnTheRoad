import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PersonaScoreMapper extends 
	Mapper<Text, Text, Text, Text> {
	
	Text persona = new Text();
	
	@Override
	public void map(Text key, Text values,
			Context context) throws IOException, InterruptedException {
		
		String[] personaSplit = values.toString().split(" ");
		StringBuilder persBuilder = new StringBuilder();
		// Puede haber personas con un apellido o con dos
		if(personaSplit.length == 3 || personaSplit.length == 4){
			if(personaSplit.length == 3){
				persBuilder.append(personaSplit[0]).append(" ").append(personaSplit[1]);
			}else {
				persBuilder.append(personaSplit[0]).append(" ").append(personaSplit[1]).append(" ").append(personaSplit[2]);
			}
			persona.set(persBuilder.toString());
			context.write(key, persona);
		}
	}
}
