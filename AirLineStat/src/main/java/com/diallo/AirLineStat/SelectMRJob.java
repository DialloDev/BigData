package com.diallo.AirLineStat;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SelectMRJob extends Configured implements Tool{

	public static class SelectMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			//sauter la ligne d'entête des titres de colonnes
			if (!AirlineDataUtil.isHeader(value)){
				StringBuilder sb = AirlineDataUtil
								   .mergeStringArray(AirlineDataUtil.getSelectedColumnsA(value), ",");
				//nullWriter a la place du key
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		//recup du job à executer avec les params de la conf
		Job job = Job.getInstance(this.getConf());
		job.setJarByClass(SelectMRJob.class);
		
		//format de fichier d'entree et de sortie 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SelectMapper.class);
		
		//indique qu'il n'y a pas de reducteur
		
		job.setNumReduceTasks(0);
		
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
		
		ToolRunner.run(new SelectMRJob(), args);
	}

	
}
