/*
 ...
*/

package com.github.currencies.batch.computing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.Tuple3;

import com.github.currencies.holder.CurrenciesHolder;
import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Main class to run full currencies count.
 *
 * @author gra
 */
public class Main {
    public static void main( String[] args ) throws UnknownHostException, IOException, GeoIp2Exception {
	    CHECK_USEGE(args);
	    JavaStreamingContext ssc = PREPARE_SPARK_CONTEXT();
	    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
	            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
	    JavaDStream<CurrenciesHolder> jsonsMapped = lines.flatMap(CurrenciesHolder.PREPARE_LINES());
	    //remove nulls
	    JavaDStream<CurrenciesHolder> jsons = jsonsMapped.filter(new Function<CurrenciesHolder, Boolean>() {
			private static final long serialVersionUID = 6735539213368102430L;
			public Boolean call(CurrenciesHolder s) {
				  return (null!=s)&&s.noAnyNull();
		    }
        });
	    JavaPairDStream<Tuple3<String, String, String>, Tuple3<Double, Double, Double>> pairs
	    	= jsons.mapToPair(new PairFunction<CurrenciesHolder
	    			, Tuple3<String, String, String>, Tuple3<Double, Double, Double>>() {
				private static final long serialVersionUID = -5800347417661881676L;
				public Tuple2<Tuple3<String, String, String>, Tuple3<Double, Double, Double>>
					call(CurrenciesHolder data) {
					return data.prepareMapToPairs();
				}});
	    JavaPairDStream<Tuple3<String, String, String>, Tuple3<Double, Double, Double>> reduced
	    	= pairs.reduceByKey(new Function2<
	    			Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>, Tuple3<Double, Double, Double>>() {
				private static final long serialVersionUID = -5793498283912220473L;
				@Override
				public Tuple3<Double, Double, Double>
					call(Tuple3<Double, Double, Double> a, Tuple3<Double, Double, Double> b) {
					// set on CurrenciesHolder fields ;)
			        return new Tuple3<Double, Double, Double>(a._1()+b._1(), a._2()+b._2(), (a._3()+b._3())/2);
			    }
		    });
		// TODO save ES instead
	    JavaDStream<Long> countreduced = reduced.count();
	    JavaDStream<Long> count = lines.count();
	    // show amount of jsons per a sec
	    count.print();
	    countreduced.print();
	    reduced.print();

	    ssc.start();
	    ssc.awaitTermination();
    }

	private static void CHECK_USEGE(String[] args) {
		if (2 != args.length) {
		      System.out.println("Error: expected: <hostname> <port>.");
		      System.exit(-1);
        }
	}

	private static JavaStreamingContext PREPARE_SPARK_CONTEXT() {
		SparkConf conf = new SparkConf().setAppName("Real time currencyF.");
		conf.set("es.nodes", "elasticsearch-1:9200");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.github.currencies.registrator.CurrenciesRegistrator");
		return new JavaStreamingContext(conf, Durations.seconds(1));
	}
}
