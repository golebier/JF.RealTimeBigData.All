package com.github.batch.computing.currencies.fasterxml.jackson;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CurrenciesTest {
	private static final String JSON_LINE_PATH
        = "src/test/resources/com/github/batch/computing/currencies/faster/json/tested-line.txt";
    private static final String JSON_STRING
    // TODO
        = "";
	private final static ObjectMapper JACKSON_MAPPER = new ObjectMapper();

	@Test
    public void shouldParseString() throws JsonParseException, JsonMappingException, IOException {
	    Map<?, ?> jsonParser = JACKSON_MAPPER.readValue(JSON_STRING, Map.class);
    	Assert.assertNotNull(jsonParser);
	    Assert.assertFalse(jsonParser.isEmpty());
	    LinkedHashMap<?, ?> request = (LinkedHashMap<?, ?>)jsonParser.get("request");
	    String path = (String)request.get("path");
	    Assert.assertNotNull(path);
    }

	@Test
    public void shouldParseStringFromFile() throws IOException {
		String json_string = FileUtils.readFileToString(new File(JSON_LINE_PATH));
	    Map<?, ?> jsonParser = JACKSON_MAPPER.readValue(json_string, Map.class);
    	Assert.assertNotNull(jsonParser);
	    Assert.assertFalse(jsonParser.isEmpty());
	    LinkedHashMap<?, ?> request = (LinkedHashMap<?, ?>)jsonParser.get("request");
	    String path = (String)request.get("path");
	    Assert.assertNotNull(path);
    }
}
