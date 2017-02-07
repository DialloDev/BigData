package com.diallo.helloHadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountMRv1 {
	public static class MonMapper extends MapReduceBase
								  implements Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable noLigne, Text ligne,
				OutputCollector<Text, IntWritable> sortie, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			if (ligne != null) {
				sortie.collect(new Text(ligne.toString().trim()), new IntWritable(1));
			}
			
		}
		
		
	}
	
	public static class MonReducteur extends MapReduceBase 
									 implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text mot, Iterator<IntWritable> decompte,
				OutputCollector<Text, IntWritable> sortie, Reporter reporter)
				throws IOException {
					
				int sum = 0;
				while (decompte.hasNext()){
					sum += decompte.next().get();
				}
				sortie.collect(new Text(mot), new IntWritable(sum));
			
		}
		
	}
	
	public static void main(String[] args) throws IOException{
		JobConf conf = new JobConf(WordCountMRv1.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(MonMapper.class);
		conf.setReducerClass(MonReducteur.class);
		conf.setCombinerClass(MonReducteur.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
	
}
