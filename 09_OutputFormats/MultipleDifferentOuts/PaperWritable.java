
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class PaperWritable implements Writable {

	Text idPaper;
	Text tituloPaper;
	
	public void set(Text idPaper, Text titulo){
		this.idPaper.set(idPaper);
		this.tituloPaper.set(titulo);
	}
	
	public void set(String idPaper, String titulo){
		this.idPaper.set(new Text(idPaper));
		this.tituloPaper.set(new Text(titulo));
	}
	
	public PaperWritable() {
		this.idPaper = new Text();
		this.tituloPaper = new Text();
	}
	
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.idPaper.readFields(arg0);
		this.tituloPaper.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		this.idPaper.write(arg0);
		this.tituloPaper.write(arg0);
	}
	
	
	@Override
	public String toString() {
		return idPaper.toString()+" "+tituloPaper.toString();
	}
	

	public Text getIdPaper() {
		return idPaper;
	}

	public void setIdPaper(Text idPaper) {
		this.idPaper = idPaper;
	}

	public Text getTituloPaper() {
		return tituloPaper;
	}

	public void setTituloPaper(Text tituloPaper) {
		this.tituloPaper = tituloPaper;
	}

	
}
