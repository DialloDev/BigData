package com.diallo.firstSPark;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.twitter.chill.java.ArraysAsListSerializer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf = new SparkConf()
        					.setAppName("wordCountSpark")
        					.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/user/Formation/wordcount2/input/miserable.txt"); 
        JavaPairRDD<String, Integer> result = lines.flatMap(new DecoupeMot())
        	 .mapToPair(new CreatePairMot())
             .reduceByKey(new AdditionneurMot());
        result.saveAsTextFile("/user/Formation/wordcount2/ouput_spark1");
    }
    
    public static class AdditionneurMot implements Function2<Integer, Integer, Integer>{

		@Override
		public Integer call(Integer compteur1 , Integer compteur2) throws Exception {
			// TODO Auto-generated method stub
			return compteur1 + compteur2;
		}
    	
    }
    
    public static class CreatePairMot implements PairFunction<String, String, Integer>{

		@Override
		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, Integer>(arg0, 1);
		}
    	
    	
    }
    
    public static class DecoupeMot implements FlatMapFunction<String, String> {

		@Override
		public Iterable<String> call(String line) throws Exception {
			String[] mots = line.split("[ -.,;!?\"$:#]+");
			return  Arrays.asList(mots);
		}
 
    }
 }
