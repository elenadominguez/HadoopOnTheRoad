

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Formato de las líneas de entrada
 * 
journals/cl/SantoNR90:::Michele Di Santo::Libero Nigro::Wilma Russo:::Programmer-Defined Control Abstractions in Modula-2.

paper-id:::author1::author2::…. ::authorN:::title
 */

public class MultipleDifferentOutsMapper  extends 
	Mapper <LongWritable, Text, Text, PaperWritable> {

	private PaperWritable tipoPaper = new PaperWritable();
	private Text autor = new Text();
		 
	 public void map(LongWritable key, Text values, Context context) 
		   throws IOException, InterruptedException{
		 
		 String[] paper = values.toString().split(":::");
		 
		 if(paper.length < 3){
			 System.out.println("HA OCURRIDO UN ERROR!! ");
			 System.out.println("HA OCURRIDO UN ERROR!! "+paper.toString());
			 System.out.println("HA OCURRIDO UN ERROR!! ");
		 }else{
			 
			 tipoPaper.set(paper[0], paper[2]);
			 for(String author : paper[1].split("::")){
				 autor.set(author);
				 context.write( autor, tipoPaper);
			 }
		 }
	 }
}
