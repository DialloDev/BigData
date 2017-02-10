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
import org.apache.hadoop.mapreduce.Partitioner;
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

public class AdvancedAggregationMRJob extends Configured implements Tool 
{
	public static final IntWritable VOL = new IntWritable(0);
	public static final IntWritable A_HEURE_ARRIVEE = new IntWritable(1);
	public static final IntWritable A_HEURE_DEPART = new IntWritable(2);
	public static final IntWritable RETARD_DEPART = new IntWritable(3);
	public static final IntWritable RETARD_ARRIVEE = new IntWritable(4);
	public static final IntWritable ANNULE = new IntWritable(5);
	public static final IntWritable DEROUTE = new IntWritable(6);
	
	
	
	public static class SelectMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			// sauter la ligne d'en-tête (titre de colonne)
			if (!AirlineDataUtil.isHeader(value)) {
				String[] champs = value.toString().split(",");
				// 0 -> mois inconnue/invalide
				int mois = AirlineDataUtil.parseMinutes(AirlineDataUtil.getMonth(champs),0);
				
				int retard_depart = AirlineDataUtil.parseMinutes(
														AirlineDataUtil.getDepartureDelay(champs),0);
				
				int retard_arrivee = AirlineDataUtil.parseMinutes(
						AirlineDataUtil.getArrivalDelay(champs),0);
				
				boolean annule = AirlineDataUtil.parseBoolean(
						AirlineDataUtil.getCancelled(champs), false);
				
				boolean deroute = AirlineDataUtil.parseBoolean(
						AirlineDataUtil.getDiverted(champs), false);
		
				// si mois invalide, on ignore cette ligne
				if (mois < 1 || mois > 12) {
					return;
				}

				IntWritable m = new IntWritable(mois);
				// je compte le vol
				context.write(m, VOL);
				
				if (retard_depart >= 5)
					context.write(m, RETARD_DEPART);
				else
					context.write(m, A_HEURE_DEPART);
				
				if (retard_arrivee >= 5)
					context.write(m, RETARD_ARRIVEE);
				else
					context.write(m, A_HEURE_ARRIVEE);
				
				if (annule)
					context.write(m, ANNULE);
				if (deroute)
					context.write(m, DEROUTE);
			}
		}
	}
	
	
	public static class AggregationReducer extends Reducer<IntWritable, IntWritable,
															NullWritable, Text> {

		@Override
		protected void reduce(
				IntWritable mois,
				Iterable<IntWritable> codes,
				Reducer<IntWritable, IntWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			double volsTotals = 0;
			double arriveAHeure = 0;
			double arriveRetard = 0;
			double departAHeure = 0;
			double departRetard = 0;
			double annules = 0;
			double deroutes = 0;
			
			for (IntWritable code : codes) {
				if (code.equals(VOL))
					volsTotals++;
				else if(code.equals(RETARD_DEPART))
					departRetard++;
				else if(code.equals(RETARD_ARRIVEE))
					arriveRetard++;
				else if(code.equals(A_HEURE_DEPART))
					departAHeure++;
				else if(code.equals(A_HEURE_ARRIVEE))
					arriveAHeure++;
				else if(code.equals(ANNULE))
					annules++;
				else if(code.equals(DEROUTE))
					deroutes++;
			}
			
			DecimalFormat df = new DecimalFormat("00.0000");
			
			StringBuilder sb = new StringBuilder(mois.toString());
			sb.append(",").append(volsTotals);
			sb.append(',').append(df.format(arriveAHeure/volsTotals * 100.0));
			sb.append(',').append(df.format(departAHeure/volsTotals * 100.0));
			sb.append(',').append(df.format(arriveRetard/volsTotals * 100.0));
			sb.append(',').append(df.format(departRetard/volsTotals * 100.0));
			sb.append(',').append(df.format(annules/volsTotals * 100.0));
			sb.append(',').append(df.format(deroutes/volsTotals * 100.0));
			
			// ecriture des statistiques du mois
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
	}
	
	public static class MoisPartitionner extends Partitioner<IntWritable, IntWritable> {

		// le retour est le numero de la partition
		@Override
		public int getPartition(IntWritable mois, IntWritable data, int nombre_partitions) {
			//return (mois.hashCode() % nombre_partitions);
			
			// 2 -> 1-6 7-12
			// 3 -> 1-4 5-8 9-12
			//
			int taille_partition = 12 / nombre_partitions;
			if (taille_partition == 0)
				taille_partition = 1;
			return ((mois.get() - 1) / taille_partition);
		}
	}
	
	

	@Override
	public int run(String[] args) throws Exception {
		// je recupere le job a executer avec les parametres de la conf
		Job job = Job.getInstance(this.getConf());
		
		job.setJarByClass(AdvancedAggregationMRJob.class);
		
		// format des fichier d'entrée et de sortie
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// sortie du mapper
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// mise en place mapper et reducteur
		job.setMapperClass(SelectMapper.class);
		job.setReducerClass(AggregationReducer.class);
		
		job.setPartitionerClass(MoisPartitionner.class);
		
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
		ToolRunner.run(new AdvancedAggregationMRJob(), args);
	}
	
	
	
}