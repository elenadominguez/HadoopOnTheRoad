import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class PersonaWritable implements Writable {

	Text nombre, primerApellido, segundoApellido;
	
	public PersonaWritable() {
		this.nombre = new Text();
		this.primerApellido = new Text();
		this.segundoApellido = new Text();
	}
	public PersonaWritable(Text nombre, Text primerApellido,
			Text segundoApellido) {
		this.nombre = nombre;
		this.primerApellido = primerApellido;
		this.segundoApellido = segundoApellido;
	}
	public PersonaWritable(String nombre, String primerApellido,
			String segundoApellido) {
		this.nombre = new Text(nombre);
		this.primerApellido = new Text(primerApellido);
		this.segundoApellido = new Text(segundoApellido);
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.nombre.readFields(arg0);
		this.primerApellido.readFields(arg0);
		this.segundoApellido.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		this.nombre.write(arg0);
		this.primerApellido.write(arg0);
		this.segundoApellido.write(arg0);
	}
	
	public Text getNombre() {
		return nombre;
	}
	public void setNombre(Text nombre) {
		this.nombre = nombre;
	}
	public Text getPrimerApellido() {
		return primerApellido;
	}
	public void setPrimerApellido(Text primerApellido) {
		this.primerApellido = primerApellido;
	}
	public Text getSegundoApellido() {
		return segundoApellido;
	}
	public void setSegundoApellido(Text segundoApellido) {
		this.segundoApellido = segundoApellido;
	}
	
	
}
