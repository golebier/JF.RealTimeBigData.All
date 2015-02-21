/*
 ...
*/

package com.github.currencies.batch.computing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

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

	    // TODO batch corrections
	    // TODO UI

	    // Ok, add more of the CurrenciesHolder fields = use CurrenciesHolder
		// save ES instead -- prepare data
	    JavaDStream<Map<String, Object>> javaRDD = reduced.map(
        		new Function<Tuple2<Tuple3<String, String, String>, Tuple3<Double, Double, Double>>, Map<String, Object>>() {
			private static final long serialVersionUID = -6509311005347136809L;
			@Override
	  		public Map<String, Object>
				call(Tuple2<Tuple3<String, String, String>, Tuple3<Double, Double, Double>> data) throws Exception {
	  			Map<String, Object> line = new HashMap<String, Object>();
	  			line.put("originatingCountry", data._1()._1());
	  			line.put("currencyFrom", data._1()._2());
	  			line.put("currencyTo", data._1()._3());
	  			line.put("amountSell", data._2()._1().toString());
	  			line.put("amountBuy", data._2()._2().toString());
	  			line.put("rate", data._2()._3().toString());
	  			return line;
	  		}
	  	});
	    // save ES instead -- save data
	    javaRDD.foreach(new Function<JavaRDD<Map<String, Object>>, Void>() {
			private static final long serialVersionUID = 1714172195345175479L;
			@Override
			public Void call(JavaRDD<Map<String, Object>> rdd) throws Exception {
				//JavaEsSpark.saveToEs(rdd, "currency_batch_spark_dev/currency_batch_spark");
				JavaEsSpark.saveToEs(rdd, "currency_streaming_spark_dev/currency_streaming_spark");
				return (Void) null;
			}
		});

	    // Debug ;)
//	    JavaDStream<Long> countreduced = reduced.count();
//	    JavaDStream<Long> count = lines.count();
//	    // show amount of jsons per a sec
//	    count.print();
//	    countreduced.print();
//	    reduced.print();

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
