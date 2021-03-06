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

public class AgregationMRJob extends Configured implements Tool{
	public static final IntWritable VOL = new IntWritable(0);
	public static final IntWritable A_HEURE_ARRIVE = new IntWritable(1);
	public static final IntWritable A_HEURE_DEPART = new IntWritable(2);
	public static final IntWritable RETARD_DEPART = new IntWritable(3);
	public static final IntWritable RETARD_ARRIVE = new IntWritable(4);
	public static final IntWritable ANNULE = new IntWritable(5);
	public static final IntWritable DEROUTE= new IntWritable(6);
	
	public static class SelectMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//sauter la ligne d'entête des titres de colonnes
			if (!AirlineDataUtil.isHeader(value)){
				String[] champs = value.toString().split(",");
				int mois = AirlineDataUtil.parseMinutes(AirlineDataUtil.getMonth(champs), 0);
				int retard_depart =  AirlineDataUtil.parseMinutes(AirlineDataUtil.getDepartureDelay(champs), 0);
				int retard_arrivee =  AirlineDataUtil.parseMinutes(AirlineDataUtil.getArrivalDelay(champs), 0);
				boolean annule = AirlineDataUtil.parseBoolean(AirlineDataUtil.getCancelled(champs), false);
				boolean deroute = AirlineDataUtil.parseBoolean(AirlineDataUtil.getDiverted(champs), false);
				
				if (mois < 1 || mois > 12){
					return;
				}
				IntWritable m = new IntWritable(mois);
				
				context.write(m, VOL);
				
				if (retard_depart >= 5)
					context.write(m, RETARD_DEPART);
				else
					context.write(m, A_HEURE_DEPART);
				if (retard_arrivee >= 5)
					context.write(m, RETARD_ARRIVE);
				else
					context.write(m, A_HEURE_ARRIVE);
				if (annule)
					context.write(m, ANNULE);
				if (deroute)
					context.write(m, DEROUTE);
			}
		}
		
	}
	
	public static class AggregationReducer extends Reducer<IntWritable, IntWritable, NullWritable, Text> {

		@Override
		protected void reduce(
				IntWritable mois,
				Iterable<IntWritable> codes,
				Reducer<IntWritable, IntWritable, NullWritable, Text>.Context context
				
				
				
				
				)
				throws IOException, InterruptedException {
				double volsTotal = 0;
				double arriveAHeure = 0;
				double arriveRetard = 0;
				double departAHeure = 0;
				double departRetard = 0;
				double annules = 0;
				double deroutes = 0;
				
				for (IntWritable code : codes) {
					if (code.equals(VOL))
						 volsTotal++;
					else if (code.equals(RETARD_DEPART))
						 departRetard++;
					else if (code.equals(RETARD_ARRIVE))
						arriveRetard++;
					else if (code.equals(A_HEURE_DEPART))
						departAHeure++;
					else if (code.equals(ANNULE))
						annules++;
					else if (code.equals(DEROUTE))
						deroutes++;
				}
				
				DecimalFormat df = new DecimalFormat("00.0000");
				StringBuilder sb = new StringBuilder(mois.toString());
				sb.append(",").append(volsTotal);
				sb.append(',').append(df.format(arriveAHeure/volsTotal * 100.0));
				sb.append(',').append(df.format(departAHeure/volsTotal * 100.0));
				sb.append(',').append(df.format(arriveRetard/volsTotal * 100.0));
				sb.append(',').append(df.format(departRetard/volsTotal * 100.0));
				sb.append(',').append(df.format(annules/volsTotal * 100.0));
				sb.append(',').append(df.format(deroutes/volsTotal * 100.0));
				
				context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		//recup du job à executer avec les params de la conf
		Job job = Job.getInstance(this.getConf());
		job.setJarByClass(AgregationMRJob.class);
		
		//format de fichier d'entree et de sortie 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SelectMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(SelectMapper.class);
		job.setReducerClass(AggregationReducer.class);
		
		//indique qu'il n'y a pas de reducteur
		
		job.setNumReduceTasks(1);
		
		//recup des params de conf 
		
		String[] arguments = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		if (status)
			return 0;
		else
			return 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new AgregationMRJob(), args);
	}
}
