package com.diallo.airlineStatjoin;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.diallo.airlineStatjoin.util.AirlineDataUtil;
import com.diallo.airlineStatjoin.util.InfosVol;
import com.diallo.airlineStatjoin.util.VolCompagnieCle;

public class CompagnieJoinMR extends Configured implements Tool {

	public static class VolMapper extends Mapper<LongWritable, Text, VolCompagnieCle, Text> {
		// reception de 1988,etc .csv, les vols

		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, VolCompagnieCle, Text>.Context context)
				throws IOException, InterruptedException {
			
			if (!AirlineDataUtil.isHeader(value)) {
				String[] champs = value.toString().split(",");
				// je recupere le code de la compagnie
				String code_compagnie = AirlineDataUtil.getUniqueCarrier(champs);
				// j'indique via la cle que la donnée qui va suivre
				// concerne la compagine designée et provient du VolMapper
				VolCompagnieCle cle = new VolCompagnieCle(VolCompagnieCle.VOL, code_compagnie);
				// les donnée transmise avec la clé
				InfosVol info = AirlineDataUtil.parseInfosVolFromText(value);
				// transfomation en texte
				Text vol = AirlineDataUtil.infosVolToText(info);
				// j'envoie la donnée
				context.write(cle, vol);
			}
		}
	}
	
	public static class CompagnieMapper extends Mapper<LongWritable, Text, VolCompagnieCle, Text> {
		// reception de carrier.csv (les informations sur les compagnies)
		
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, VolCompagnieCle, Text>.Context context)
				throws IOException, InterruptedException {
			String[] detailsCompagnie = AirlineDataUtil.extraitDetailsCompagnie(value);
			// clé indiquant la provenance de ce mapper, et fournissant le
			// code compagnie correspondant
			VolCompagnieCle cle = new VolCompagnieCle(VolCompagnieCle.COMPAGNIE,
													  detailsCompagnie[0].trim());
			// le code avec le nom associé de la compagnie
			Text infoCompagnie = new Text(detailsCompagnie[0] + "#" + detailsCompagnie[1]);
			context.write(cle, infoCompagnie);
		}
	}

	public static class JoinReducer extends Reducer<VolCompagnieCle, Text, NullWritable, Text> {

		private String compagnieCourante = "inconnue";
		
		// ce reducteur sera appelé
		// 	1 -> je recois en provenance de compagnie
		//		 1 seule ligne avec les details de la compagnie
		//		 et, au prochain appel du reducteur
		//		 je recevrait tous les vols de cette compagnie
		//  2 -> je recois en provenance de vol
		//		 n ligne (tous les vols) et NORMALEMENT
		//       j'avais recu juste avant les details de la compagnie
		@Override
		protected void reduce(VolCompagnieCle cle, Iterable<Text> data,
				Reducer<VolCompagnieCle, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (Text ligne : data) {
				if (cle.provenance_ligne.get() == VolCompagnieCle.VOL) {
					// c'est un details d'un vol
					// j'ajout un champ avec le nom de la compagnie
					Text infos = new Text(ligne.toString()+ ",\"" + compagnieCourante + "\"");
					// et j'ecris dans le résultat
					context.write(NullWritable.get(), infos);
				}
				else {
					// c'est les information d'une compagnie
					// je recupere le nom de la compagnie
					compagnieCourante = ligne.toString().split("#")[1];
				}
			}
			
		}
	}

	public static class CompaniePartitioneur extends Partitioner<VolCompagnieCle, Text> {

		@Override
		public int getPartition(VolCompagnieCle cle, Text value, int nbPartitions) {
			// valeur absolue safe pour eviter un modulo negatif
			return (cle.code_compagnie.hashCode() & Integer.MAX_VALUE) % nbPartitions;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(CompagnieJoinMR.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String[] arguments = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		// premier chemin de lecture de fichier, pour les vols -> VolMapper
		MultipleInputs.addInputPath(job,
									new Path(arguments[0]),
									TextInputFormat.class,
									VolMapper.class);
		
		// deuxieme chemin de lecture de fichier, pour les compagnies -> CompagnieMapper
		MultipleInputs.addInputPath(job,
									new Path(arguments[1]),
									TextInputFormat.class,
									CompagnieMapper.class);
		
		// repertoire de sortie
		FileOutputFormat.setOutputPath(job, new Path(arguments[2]));
		
		job.setMapOutputKeyClass(VolCompagnieCle.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(JoinReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// capital!!! pour bien envoyer tous les enregistrement, de vol et compagnie
		// qui concerne la meme compagnie vers le même réducteur
		job.setPartitionerClass(CompaniePartitioneur.class);
		
		boolean status = job.waitForCompletion(true);
		if (status)
			return 0;
		else
			return 1;
	}
	

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CompagnieJoinMR(), args);
	}
	
}
