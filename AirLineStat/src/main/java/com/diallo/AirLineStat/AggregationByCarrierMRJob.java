package com.diallo.AirLineStat;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * exemple d'un selecte simple sur nos données
 * 
 */

public class AggregationByCarrierMRJob extends Configured implements Tool 
{
	
	
	
	public static class SelectMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			// sauter la ligne d'en-tête (titre de colonne)
			if (!AirlineDataUtil.isHeader(value)) {
				String[] champs = value.toString().split(",");
				
				String compagnie = AirlineDataUtil.getUniqueCarrier(champs);
				String duree_reelle = AirlineDataUtil.getElapsedTime(champs);
				String duree_prevue = AirlineDataUtil.getScheduledElapsedTime(champs);
				String duree_distance = AirlineDataUtil.getDistance(champs);
				Text values = new Text(duree_reelle + "," + duree_prevue + "," + duree_distance);
				
				context.write(new Text(compagnie), values);
			}
		}
	}
	
	
	public static class AggregationReducer extends Reducer<Text, Text,
															NullWritable, Text> {

		@Override
		protected void reduce(
				Text code_compagnie,
				Iterable<Text> duree_vols,
				Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			double nombre_vols = 0;
			double duree_reelle_total = 0;
			double difference_duree_total = 0;
			
			for (Text duree : duree_vols) {
				String[] champs = duree.toString().split(",");
				
				int duree_reelle = AirlineDataUtil.parseMinutes(champs[0], 0);
				int duree_prevue = AirlineDataUtil.parseMinutes(champs[1], 0);
				if (duree_reelle == 0 || duree_prevue == 0)
					continue;
				
				duree_reelle_total += duree_reelle;
				difference_duree_total += Math.abs(duree_reelle - duree_prevue);
				
				nombre_vols++;
			}
			
			DecimalFormat df = new DecimalFormat("0000.00");
			
			StringBuilder sb = new StringBuilder(code_compagnie.toString());
			sb.append(',').append(nombre_vols);
			sb.append(',').append(df.format(duree_reelle_total / nombre_vols));
			sb.append(',').append(df.format(difference_duree_total / nombre_vols));
			
			// ecriture des statistiques du mois
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
	}
	
	

	@Override
	public int run(String[] args) throws Exception {
		// je recupere le job a executer avec les parametres de la conf
		Job job = Job.getInstance(this.getConf());
		
		job.setJarByClass(AggregationByCarrierMRJob.class);
		
		// format des fichier d'entrée et de sortie
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// sortie du mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// mise en place mapper et reducteur
		job.setMapperClass(SelectMapper.class);
		job.setReducerClass(AggregationReducer.class);
		
		job.setNumReduceTasks(2);
		
		// recupération des parametres/configuration
		
		String[] arguments = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();
		
		FileInputFormat.setInputPaths(job, new Path(arguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
		
		// true -> mode verbose
		boolean status = job.waitForCompletion(true);
		if (status)
			return 0;
		else
			return 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		// comme notre classe implement l'interface tools
		// on est censé l'executé via un ToolRunner
		ToolRunner.run(new AggregationByCarrierMRJob(), args);
	}
	
	
	
}