/*
 ...
*/

package com.github.currencies.batch.computing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
	    JavaDStream<CurrenciesHolder> jsons = lines.flatMap(CurrenciesHolder.PREPARE_LINES());
	    
		// TODO save ES instead
//		computeAndSave(withoutNulls);

//		DatabaseReaderWrapper.close();
	    jsons.print();
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
		SparkConf conf = new SparkConf().setAppName("Batch Analytics");
		conf.set("es.nodes", "elasticsearch-1:9200");
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		conf.set("spark.kryo.registrator", "com.github.currencies.batch.computing.AnalyticsRegistrator");
		return new JavaStreamingContext(conf, Durations.seconds(1));
	}
}
