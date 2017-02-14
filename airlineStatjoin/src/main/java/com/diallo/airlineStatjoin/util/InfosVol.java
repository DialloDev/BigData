package com.diallo.airlineStatjoin.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class InfosVol {
	public static final int NORMAL = 0;
	public static final int CANCELLED = 1;
	public static final int DIVERTED = 2;
	
	
	public IntWritable annee = new IntWritable();
	public IntWritable mois = new IntWritable();
	public IntWritable retardDepart = new IntWritable();
	public IntWritable retardArrive = new IntWritable();
	public IntWritable jour = new IntWritable();
	public Text AeroportDepart = new Text();
	public Text AeroportArrive = new Text();
	public Text Compagnie = new Text();
	public IntWritable statutVol = new IntWritable();
}
