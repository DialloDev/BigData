package com.diallo.airlineStatjoin.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class VolCompagnieCle implements WritableComparable<VolCompagnieCle>{
	public static final int VOL =  1;
	public static final int COMPAGNIE =  1;
	
	public IntWritable provenance_ligne = new IntWritable();
	public Text code_compagnie= new Text();
	
	public VolCompagnieCle(){};
	public VolCompagnieCle(int provenance, String code_compagnie)
	{
		this.provenance_ligne.set(provenance);
		this.code_compagnie.set(code_compagnie);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((code_compagnie == null) ? 0 : code_compagnie.hashCode());
		result = prime
				* result
				+ ((provenance_ligne == null) ? 0 : provenance_ligne.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VolCompagnieCle other = (VolCompagnieCle) obj;
		if (code_compagnie == null) {
			if (other.code_compagnie != null)
				return false;
		} else if (!code_compagnie.equals(other.code_compagnie))
			return false;
		if (provenance_ligne == null) {
			if (other.provenance_ligne != null)
				return false;
		} else if (!provenance_ligne.equals(other.provenance_ligne))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput fluxEntre) throws IOException {
		this.provenance_ligne.readFields(fluxEntre);
		this.code_compagnie.readFields(fluxEntre);
		
	}

	@Override
	public void write(DataOutput fluxSortie) throws IOException {
		this.provenance_ligne.write(fluxSortie);
		this.code_compagnie.write(fluxSortie);
		
	}

	@Override
	public int compareTo(VolCompagnieCle o) {
		int comparaison = this.code_compagnie.compareTo(o.code_compagnie);
		if (comparaison != 0) {
			return comparaison;
		}
		return this.provenance_ligne.compareTo(provenance_ligne);
	}
	
	
	
}
