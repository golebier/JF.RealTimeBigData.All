/*
 ...
*/

package com.github.currencies.batch;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.boon.json.JsonFactory;
import org.boon.json.JsonParserAndMapper;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;
import org.testng.Assert;

import scala.Tuple2;

import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Main class to run full currencies count.
 * @author gra
 */
public class Main {
    public static void main( String[] args ) throws UnknownHostException, IOException, GeoIp2Exception {
	    JavaSparkContext sc = PREPARE_SPARK_CONTEXT();
		loadAndTest(sc, "currencies_batch_dev/currencies_batch_spark", "preparing_validation_test_results_currencies");
		sc.stop();
    }

    private static void loadAndTest(JavaSparkContext sc, String esIndexName, String validDataPath) {
		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, esIndexName);
		JavaRDD<String> validationData = sc.textFile(validDataPath);
		long sessionsReadcount = esRDD.count();
		long validationDatacount = validationData.count();
		Assert.assertEquals(sessionsReadcount, validationDatacount, "TEST began:"+esIndexName+", "+validDataPath);
		JavaPairRDD<String, Tuple2<String, Map<String, Object>>> cesRDD = validationData.cartesian(esRDD);
		JavaPairRDD<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>> maped = cesRDD.mapToPair(
				new PairFunction<Tuple2<String, Tuple2<String, Map<String, Object>>>
				, String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>>() {
			private static final long serialVersionUID = 5660772802046535070L;
			@SuppressWarnings("unchecked")
			public Tuple2<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>> call(
						Tuple2<String, Tuple2<String, Map<String, Object>>> data) {

				JsonParserAndMapper PARSER = JsonFactory.create().parser();
			    Map<String, Object> all = PARSER.parseMap(data._1());

			    JSONObject json = new JSONObject();
			    json.putAll(data._2()._2());
			    Map<String, Object> corrected = PARSER.parseMap(json.toJSONString());

			    if (corrected.size() != all.size()) {
			    	new Tuple2<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>>(
							data._1(), new Tuple2<Tuple2<String, Map<String, Object>>, Boolean>(data._2(), false)
						);
			    }
			    boolean equals = true;
			    for (String key : corrected.keySet()) {
			    	if (!new StringBuilder().append(corrected.get(key)).toString().equals(all.get(key))) {
			    		equals = false;
			    		break;
			    	}
			    }

				return new Tuple2<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>>(
							data._1(), new Tuple2<Tuple2<String, Map<String, Object>>, Boolean>(data._2(), equals)
						);
		}});
		JavaPairRDD<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>> filteredEsRDD = maped.filter(
				new Function<Tuple2<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>>, Boolean>() {
			private static final long serialVersionUID = 458908737415325438L;
			public Boolean call(Tuple2<String, Tuple2<Tuple2<String, Map<String, Object>>, Boolean>> s) {
				return s._2()._2();
		    }
        });
		long filteredEsRDDcount = filteredEsRDD.count();
		Assert.assertEquals(sessionsReadcount, filteredEsRDDcount, "TEST ended:"+esIndexName+", "+validDataPath);
	}

	private static JavaSparkContext PREPARE_SPARK_CONTEXT() {
		SparkConf conf = new SparkConf().setAppName("Batch Analytics");
		conf.set("es.nodes", "elasticsearch-1:9200");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.github.batch.currencies.registrator.AnalyticsRegistrator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}
}
