package com.diallo.helloHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMRv2 {
	public static class MonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if (value != null ) {
				context.write(new Text(value.toString().trim()), new IntWritable(1));
			}
		}
		
	}
	
	public static class MonReducteur extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(WordCountMRv2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MonMapper.class);
		job.setReducerClass(MonReducteur.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		
		if (status) {
			System.out.println("Well done !");
			System.exit(0);
		}
		else {
			System.out.println("catastrophe !!!");
			System.exit(1);
		}
	}
	
}
