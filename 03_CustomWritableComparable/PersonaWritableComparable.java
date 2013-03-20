import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class PersonaWritableComparable 
		implements WritableComparable<PersonaWritableComparable>{

	Text nombre, primerApellido, segundoApellido;
	
	public void set(String nom, String prApell, String sgApell){
		nombre.set(nom);
		primerApellido.set(prApell);
		segundoApellido.set(sgApell);
	}
	
	public PersonaWritableComparable() {
		this.nombre = new Text();
		this.primerApellido = new Text();
		this.segundoApellido = new Text();
	}

	public PersonaWritableComparable(Text nombre, 
			Text primerApellido, Text segundoApellido) {
		this.nombre = nombre;
		this.primerApellido = primerApellido;
		this.segundoApellido = segundoApellido;
	}
	
	public PersonaWritableComparable(String nombre, 
			String primerApellido, String segundoApellido) {
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

	@Override
	public int compareTo(PersonaWritableComparable o) {
		if(this.nombre.compareTo(o.nombre) != 0){
			return this.nombre.compareTo(o.nombre);
		}else if(this.primerApellido.compareTo(o.primerApellido) != 0){
			return this.primerApellido.compareTo(o.primerApellido);
		}else if(this.segundoApellido.compareTo(o.segundoApellido) != 0){
			return this.segundoApellido.compareTo(o.segundoApellido);
		}
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PersonaWritableComparable){
			PersonaWritableComparable p = (PersonaWritableComparable) obj;
			return this.nombre.equals(p.nombre) && 
					this.primerApellido.equals(p.primerApellido) && 
					this.segundoApellido.equals(p.segundoApellido);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.nombre.hashCode()*163 + 
				this.primerApellido.hashCode()*163 + 
				this.segundoApellido.hashCode()*163;
	}
	
	@Override
	public String toString() {
		return nombre.toString()+" "+primerApellido.toString()+" "+segundoApellido.toString();
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






/*
 * 
 * import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class PersonaWritableComparable 
		implements WritableComparable<PersonaWritableComparable>{

	String nombre, primerApellido, segundoApellido;
	
	public void set(String nom, String prApell, String sgApell){
		nombre = nom;
		primerApellido = prApell;
		segundoApellido = sgApell;
	}
	
	/*public PersonaWritableComparable() {
		this.nombre = new Text();
		this.primerApellido = new Text();
		this.segundoApellido = new Text();
	}

	public PersonaWritableComparable(Text nombre, 
			Text primerApellido, Text segundoApellido) {
		this.nombre = nombre;
		this.primerApellido = primerApellido;
		this.segundoApellido = segundoApellido;
	}
	
	public PersonaWritableComparable(String nombre, 
			String primerApellido, String segundoApellido) {
		this.nombre = new Text(nombre);
		this.primerApellido = new Text(primerApellido);
		this.segundoApellido = new Text(segundoApellido);
	}
	
	@Override
	public void readFields(DataInput arg) throws IOException {
		/*this.nombre.readFields(arg0);
		this.primerApellido.readFields(arg0);
		this.segundoApellido.readFields(arg0);
		
		this.nombre = arg.readUTF();
		this.primerApellido = arg.readUTF();
		this.segundoApellido = arg.readUTF();
	}

	@Override
	public void write(DataOutput arg) throws IOException {
		arg.writeUTF(nombre);
		arg.writeUTF(primerApellido);
		arg.writeUTF(segundoApellido);
		/*this.nombre.write(arg0);
		this.primerApellido.write(arg0);
		this.segundoApellido.write(arg0);
	}

	@Override
	public int compareTo(PersonaWritableComparable o) {
		if(this.nombre.compareTo(o.nombre) != 0){
			return this.nombre.compareTo(o.nombre);
		}else if(this.primerApellido.compareTo(o.primerApellido) != 0){
			return this.primerApellido.compareTo(o.primerApellido);
		}else if(this.segundoApellido.compareTo(o.segundoApellido) != 0){
			return this.segundoApellido.compareTo(o.segundoApellido);
		}
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PersonaWritableComparable){
			PersonaWritableComparable p = (PersonaWritableComparable) obj;
			return this.nombre == p.nombre && 
					this.primerApellido == p.primerApellido && 
					this.segundoApellido == p.segundoApellido;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.nombre.hashCode()*163 + 
				this.primerApellido.hashCode()*163 + 
				this.segundoApellido.hashCode()*163;
	}

	
	
	@Override
	public String toString() {
		return nombre+" "+primerApellido+" "+segundoApellido;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getPrimerApellido() {
		return primerApellido;
	}

	public void setPrimerApellido(String primerApellido) {
		this.primerApellido = primerApellido;
	}

	public String getSegundoApellido() {
		return segundoApellido;
	}

	public void setSegundoApellido(String segundoApellido) {
		this.segundoApellido = segundoApellido;
	}

	

}

 * 
 * ***/
