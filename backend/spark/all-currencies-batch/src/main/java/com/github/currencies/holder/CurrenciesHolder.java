package com.github.currencies.holder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Currency main class, with all fields currencies can contain.
 * Only currency line.
 *
 * @author gra
 */
public class CurrenciesHolder {
	// FIXME first time since 2011 joda datetime failed me ;) pattern parser I must use
	private static final DateTimeFormatter DATE_TIME_FORMATTER_LOAD = DateTimeFormat.forPattern("dd-MM-yy HH:mm:ss");
	private static Map<String, String> monthMapping = new HashMap<String, String>() {
		private static final long serialVersionUID = -5658927049934710251L;
		{
            put("JAN", "01");
            put("FEB", "02");
            put("MAR", "03");
            put("APR", "04");
            put("MAY", "05");
            put("JUN", "06");
            put("JUL", "07");
            put("AUG", "08");
            put("SEP", "09");
            put("OCT", "10");
            put("NOV", "11");
            put("DEC", "12");
        }
    };

	String userId;
	// enum?
	String currencyFrom;
	String currencyTo;
	String originatingCountry; // geoip ;)
	Double amountSell;
	Double amountBuy;
	Double rate; // could be counted: amountBuy/amountSell, or any combination of those.
	DateTime timePlaced;

	public static FlatMapFunction<String, CurrenciesHolder> PREPARE_LINES() {
		return new FlatMapFunction<String, CurrenciesHolder>() {
			private static final long serialVersionUID = 791964479110084749L;
			public Iterable<CurrenciesHolder> call(String s) throws UnknownHostException, IOException, GeoIp2Exception {
				return Arrays.asList(CurrenciesHolder.parseFromCurrencyJson(s));
            }
        };
	}
	
	public static CurrenciesHolder parseFromCurrencyJson(String json) throws UnknownHostException, IOException, GeoIp2Exception {
		JSONObject obj = (JSONObject)JSONValue.parse(json);
		if (null == obj) {
	    	return null;
	    }
		String userId = (String)obj.get("userId");
		if (null == userId) {
	    	return null;
	    }
		String currencyFrom = (String)obj.get("currencyFrom");
		if (null == currencyFrom) {
	    	return null;
	    }
		String currencyTo = (String)obj.get("currencyTo");
		if (null == currencyTo) {
	    	return null;
	    }
		String originatingCountry = (String)obj.get("originatingCountry");
		if (null == originatingCountry) {
	    	return null;
	    }
		// TODO improve this mapping
		Object amountSellObject = obj.get("amountSell");
		if (null == amountSellObject) {
			amountSellObject = new Double(0);
	    }
		Double amountSell = 0D;
		if (amountSellObject instanceof Long) {
			amountSell = ((Long) amountSellObject).doubleValue();
		}
		Double amountBuy = (Double)obj.get("amountBuy");
		if (null == amountBuy) {
			amountBuy = 0D;
	    }
		Double rate = (Double)obj.get("rate");
		if (null == rate) {
			rate = 0D;
	    }
		String date = (String)obj.get("timePlaced");
		if (null == date) {
	    	return null;
	    }
		// TODO I found a bug in joda date time, !@#$%^&*() to day ;)
		// I'll try to fix it on joda github fork, but not to day ;)
		for (String key : monthMapping.keySet()) {
			date = date.replaceAll(key, monthMapping.get(key));
    	}
		DateTime timePlaced = DATE_TIME_FORMATTER_LOAD.parseDateTime(date);
		return new CurrenciesHolder(userId, currencyFrom, currencyTo
				, originatingCountry, amountSell, amountBuy, rate, timePlaced);
	}

	public static FlatMapFunction<String, CurrenciesHolder> FLAT_MAP_JSONS() throws IOException {
		return new FlatMapFunction<String, CurrenciesHolder>() {
			private static final long serialVersionUID = 1732752477229114345L;
			public Iterable<CurrenciesHolder> call(String s) throws UnknownHostException, IOException, GeoIp2Exception {
				return Arrays.asList(CurrenciesHolder.parseFromCurrencyJson(s));
            }
        };
	}

	public static Function<CurrenciesHolder, Boolean> FILTER_NOT_NULL() {
		return new Function<CurrenciesHolder, Boolean>() {
			private static final long serialVersionUID = 866326543732464144L;
			public Boolean call(CurrenciesHolder s) {
				  return (null!=s);
		    }
        };
	}

	// avr amount sell/buy,  currency, ..., make fun ;)
	private CurrenciesHolder(String userId, String currencyFrom, String currencyTo, String originatingCountry
			, Double amountSell, Double amountBuy, Double rate, DateTime timePlaced) {
		this.userId = userId;
		this.currencyFrom = currencyFrom;
		this.currencyTo = currencyTo;
		this.originatingCountry = originatingCountry;
		this.amountSell = amountSell;
		this.amountBuy = amountBuy;
		this.rate = rate;
		this.timePlaced = timePlaced;
	}

    public String getUserId() {
		return userId;
	}
	public String getCurrencyFrom() {
		return currencyFrom;
	}
	public String getCurrencyTo() {
		return currencyTo;
	}
	public String getOriginatingCountry() {
		return originatingCountry;
	}
	public Double getAmountSell() {
		return amountSell;
	}
	public Double getAmountBuy() {
		return amountBuy;
	}
	public Double getRate() {
		return rate;
	}
	public DateTime getTimePlaced() {
		return timePlaced;
	}

	@Override
	public String toString() {
		return String.format("userId %s, currencyFrom %s, currencyTo %s, originatingCountry %s, amountSell %f, amountBuy %f, rate %f, timePlaced %d"
			   , userId, currencyFrom, currencyTo, originatingCountry, amountSell, amountBuy, rate, timePlaced.getMillis());
	}
}
