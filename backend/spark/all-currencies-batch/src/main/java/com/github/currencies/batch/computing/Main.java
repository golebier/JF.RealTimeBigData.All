/*
 ...
*/

package com.github.currencies.batch.computing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.github.currencies.holder.CurrenciesHolder;
import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Main class to run full currencies count.
 *
 * @author gra
 */
public class Main {
//	private static final Pattern SPACE = Pattern.compile(" ");
//
//	  public static void main(String[] args) {
//	    if (args.length < 2) {
//	      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
//	      System.exit(1);
//	    }
//
//	    StreamingExamples.setStreamingLogLevels();
//
//	    // Create the context with a 1 second batch size
//	    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
//	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
//
//	    // Create a JavaReceiverInputDStream on target ip:port and count the
//	    // words in input stream of \n delimited text (eg. generated by 'nc')
//	    // Note that no duplication in storage level only for running locally.
//	    // Replication necessary in distributed scenario for fault tolerance.
//	    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
//	            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
//	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//	      @Override
//	      public Iterable<String> call(String x) {
//	        return Lists.newArrayList(SPACE.split(x));
//	      }
//	    });
//	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//	      new PairFunction<String, String, Integer>() {
//	        @Override
//	        public Tuple2<String, Integer> call(String s) {
//	          return new Tuple2<String, Integer>(s, 1);
//	        }
//	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//	        @Override
//	        public Integer call(Integer i1, Integer i2) {
//	          return i1 + i2;
//	        }
//	      });
//
//	    wordCounts.print();
//	    ssc.start();
//	    ssc.awaitTermination();
//	  }

    public static void main( String[] args ) throws UnknownHostException, IOException, GeoIp2Exception {
	    CHECK_USEGE(args);
	    JavaSparkContext sc = PREPARE_SPARK_CONTEXT();
	    String logFilePath = args[0];

	    // USE Spark to generate those jsons ;)
	    
	    // TODO {"userId": "134256", "currencyFrom": "EUR", "currencyTo": "GBP", "amountSell": 1000, "amountBuy": 747.10
	    //       , "rate": 0.7471, "timePlaced" : "14-JAN-15 10:27:44", "originatingCountry" : "FR"}

	    // TODO Stream from Flume
	    JavaRDD<String> currencies = sc.textFile(logFilePath);
		JavaRDD<CurrenciesHolder> parts = currencies.flatMap(CurrenciesHolder.FLAT_MAP_JSONS());
		JavaRDD<CurrenciesHolder> withoutNulls = parts.filter(CurrenciesHolder.FILTER_NOT_NULL());

		// TEST generate N jsons per T ime with random fields
//		JavaRDD<CurrenciesHolder> newLines = withoutNulls.map(new Function<CurrenciesHolder, CurrenciesHolder>() {
//			private static final long serialVersionUID = -6170423912286687076L;
//			public CurrenciesHolder call(CurrenciesHolder s) {
//				  return s;
//		    }}
//        );

	    JavaRDD<String> logLines = sc.textFile("1line.log");
		int NUM_SAMPLES = 216000000;// -- 60 000 per sec by hour, 1440000000 -- 400 000 per sec by hour
    	double renge = Math.sqrt(NUM_SAMPLES)+1;
	    // get a length that will give a base for join
    	JavaRDD<Integer> parallelized0 = sc.parallelize(makeRange(0, (int)renge)).filter(FILTER());
    	JavaRDD<Integer> parallelized1 = sc.parallelize(makeRange(0, (int)renge)).filter(FILTER());
    	JavaPairRDD<Integer, Integer> cartesianed = parallelized0.cartesian(parallelized1);
		JavaPairRDD<Tuple2<Integer, Integer>, String> paired = cartesianed.cartesian(logLines);
		JavaRDD<String> randomized = paired.map(new Function<Tuple2<Tuple2<Integer, Integer>, String>, String>() {
			private static final long serialVersionUID = 8910642060539381814L;
			@Override
			public String call(Tuple2<Tuple2<Integer, Integer>, String> data) throws Exception {
				// TODO make it random
				CurrenciesHolder ch = CurrenciesHolder.parseFromCurrencyJson(data._2());
				String randomized = ch.ramdomize();
				return data._2();
			}
			
		});

		long count = randomized.count();
		System.out.println("------------------- "+count+", "+NUM_SAMPLES);
		
//		computeAndSave(withoutNulls);

		sc.stop();
//		DatabaseReaderWrapper.close();
    }

	private static Function<Integer, Boolean> FILTER() {
		return new Function<Integer, Boolean>() {
			private static final long serialVersionUID = -7482464778722345646L;
			public Boolean call(Integer i) {
			    return true;// Math.random()
			}
		};
	}
    
	private static List<Integer> makeRange(int i, int size) {
		List<Integer> out = new ArrayList<Integer>();
		for (; i < size; ++i) {
			out.add((int)Math.random());
		}
		return out;
	}
    // DONE: Message Consumption & Message Processor in one, Spark is ok with that, Test Flume ...

	private static void CHECK_USEGE(String[] args) {
		if (2 != args.length) {
		      System.out.println("Error: expected: <hostname> <port>.");
		      System.exit(-1);
        }
	}

	private static JavaSparkContext PREPARE_SPARK_CONTEXT() {
		SparkConf conf = new SparkConf().setAppName("Batch Analytics");
		conf.set("es.nodes", "elasticsearch-1:9200");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.github.currencies.batch.computing.AnalyticsRegistrator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}
}
