package com.github.currencies.holder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * Currency unit tests.
 *
 * @author gra
 */
public class CurrentiesHolderTest {
	private static final String JSON_STRING
    	= "{\"userId\": \"134256\", \"currencyFrom\": \"EUR\", \"currencyTo\": \"GBP\", \"amountSell\": 1000.0, \"amountBuy\": 747.10, \"rate\": 0.7471, \"timePlaced\" : \"14-JAN-15 10:27:44\", \"originatingCountry\" : \"FR\"}";
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

	// TODO test all wrong can happen, nulls, exceptions, ...
	// TODO test and improve numbers to Double mapping

    // men, to day I must found that bug? !@#$%^&*()
    @Test
    public void shouldCorrectlyParseJsonToCurrentiesHolderTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertNotNull(currentiesHolder);
    }

    @Test
    public void shouldParsedCurrentiesHolderUserIdTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getUserId(), "134256");
    }

    @Test
    public void shouldParsedCurrentiesHolderCurrencyFromTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getCurrencyFrom(), "EUR");
    }

    @Test
    public void shouldParsedCurrentiesHolderCurrencyToTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getCurrencyTo(), "GBP");
    }

    @Test
    public void shouldParsedCurrentiesHolderOriginatingCountryTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getOriginatingCountry(), "FR");
    }

    @Test
    public void shouldParsedCurrentiesHolderAmountSellTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getAmountSell(), 1000.0D);
    }

    @Test
    public void shouldParsedCurrentiesHolderAmountBuyTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getAmountBuy(), 747.10D);
    }

    @Test
    public void shouldParsedCurrentiesHolderRateTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getRate(), 0.7471D);
    }

    @Test
    public void shouldParsedCurrentiesHolderTimePlacedTest() throws UnknownHostException, IOException, GeoIp2Exception {
    	CurrenciesHolder currentiesHolder = CurrenciesHolder.parseFromCurrencyJson(JSON_STRING);
    	Assert.assertEquals(currentiesHolder.getTimePlaced(), new DateTime(2015, 01, 14, 10, 27, 44));
    }

    // test for a joda date time format-er bug
    @Test
    public void cleanDateTest() throws ParseException {
    	Assert.assertNotNull(DateTime.parse(fixBugyMMM("14-JAN-15 10:27:44"), DateTimeFormat.forPattern(("dd-MM-yy HH:mm:ss"))));
    }

    private String fixBugyMMM(String bugyDate) {
    	for (String key : monthMapping.keySet()) {
    		bugyDate = bugyDate.replaceAll(key, monthMapping.get(key));
    	}
    	return bugyDate;
    }
}
