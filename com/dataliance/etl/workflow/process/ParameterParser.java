package com.dataliance.etl.workflow.process;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import java.io.*;
import org.codehaus.jackson.type.*;
import java.util.*;

public class ParameterParser
{
    public static List<LinkedHashMap<String, Object>> convertJson2List(final String messageConent) throws JsonParseException, JsonMappingException, IOException {
        List<LinkedHashMap<String, Object>> parameters = new ArrayList<LinkedHashMap<String, Object>>();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        parameters = (List<LinkedHashMap<String, Object>>)objectMapper.readValue(messageConent, (Class)List.class);
        return parameters;
    }
    
    public static Map<String, String> convertJson2Map(final String messageConent) throws JsonParseException, JsonMappingException, IOException {
        Map<String, String> parameters = new HashMap<String, String>();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        parameters = (Map<String, String>)objectMapper.readValue(messageConent, (TypeReference)new TypeReference<Map<String, String>>() {});
        return parameters;
    }
    
    public static void main(final String[] args) throws JsonParseException, JsonMappingException, IOException {
        String json2 = "{success:true, test:value}";
        json2 = "{'ID_from':'/opt/a','OD_target':'/opt/demo/result','OD_targetlog':'/opt/clean.log','DC_outputsplit':'*','DC_startcmd':'/opt/bin/start.sh','DC_datadic':'19','DC_inputsplit':'*'}";
        json2 = "{'DC_outputsplit':'@#$','DC_startcmd':'/opt/brainbook/bigdata-core/start-dataclean.sh','DC_datadic':'19','DC_inputsplit':'@#\\\\$','ID_from':'/user/demo/wapdata','OD_target':'/user/demo/result','OD_targetlog':'/home/demo/data-clean.log'}";
        final Map<String, String> maps = convertJson2Map(json2);
        final Set<String> key = maps.keySet();
        for (final String field : key) {
            System.out.println(field + ":" + maps.get(field));
        }
    }
}
