package com.github.batch.computing.currencies.faster.json;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.boon.core.value.LazyValueMap;
import org.boon.json.JsonFactory;
import org.boon.json.JsonParserAndMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CurrenciesTest {
	private static final String JSON_LINE_PATH
        = "src/test/resources/com/github/batch/computing/currencies/faster/json/tested-line.txt";
    private static final String JSON_STRING
    // TODO
        = "";
	private final static JsonParserAndMapper PARSER = JsonFactory.create().parser();//new JsonParserFactory().create();

	@Test
    public void shouldParseString() {
    	Map<String, Object> all = PARSER.parseMap(JSON_STRING);
    	Assert.assertNotNull(all);
	    Assert.assertFalse(all.isEmpty());
	    LazyValueMap request = (LazyValueMap)all.get("request");
	    String path = (String)request.get("path");
	    Assert.assertNotNull(path);
    }

	@Test
    public void shouldParseStringFromFile() throws IOException {
		String json_string = FileUtils.readFileToString(new File(JSON_LINE_PATH));
    	Map<String, Object> all = PARSER.parseMap(json_string);
    	Assert.assertNotNull(all);
	    Assert.assertFalse(all.isEmpty());
	    LazyValueMap request = (LazyValueMap)all.get("request");
	    String path = (String)request.get("path");
	    Assert.assertNotNull(path);
    }
}
