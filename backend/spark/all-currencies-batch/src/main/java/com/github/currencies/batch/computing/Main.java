/*
 ...
*/

package com.github.currencies.batch.computing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Main class to run full currencies count.
 *
 * @author gra
 */
public class Main {
	private static final DateTimeFormatter DATE_TIME_FORMATTER_SAVE = DateTimeFormat.forPattern("yyyyMMddHHmm");
    public static void main( String[] args ) throws UnknownHostException, IOException, GeoIp2Exception {
	    CHECK_USEGE(args);
	    JavaSparkContext sc = PREPARE_SPARK_CONTEXT();
	    String logFilePath = args[0];

	    // TODO exclude kpis to class with methods and all
	    JavaRDD<String> logLines = sc.textFile(logFilePath);
		JavaRDD<CurrenciesHolder> parts = logLines.flatMap(CurrenciesHolder.PREPARE_LINES());
		// TODO Boolean invalidData instead of nulls ;)
		JavaRDD<CurrenciesHolder> withoutNulls = parts.filter(CurrenciesHolder.PREPARE_NOT_NULL_LINES());

		computeAndSaveSessions(withoutNulls);
		computeAndSaveMinutes(withoutNulls);

		sc.stop();
		DatabaseReaderWrapper.close();
    }

    // TODO line: {"userId": "134256", "currencyFrom": "EUR", "currencyTo": "GBP", "amountSell": 1000, "amountBuy": 747.10, "rate": 0.7471, "timePlaced" : "14-JAN-15 10:27:44", "originatingCountry" : "FR"}
    //      to CurrenciesHolder
    // count ...
    // DONE: Message Consumption & Message Processor in one, Spark is ok with that, Test Flume ...

	private static void CHECK_USEGE(String[] args) {
		if (2 != args.length) {
		      System.out.println("Must specify an access logs file and output path.");
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
