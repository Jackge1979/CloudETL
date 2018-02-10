package com.dataliance.etl.workflow.process;

import java.util.*;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import java.io.*;

public class TestParameter
{
    public static void main(final String[] args) throws JsonParseException, JsonMappingException, IOException {
        String parameter = "{DC-inputsplit:'@#$',DC-datadic:'2',DC-startcmd:'/opt/brainbook/bigdata-core/xmonitor.sh',DC-outputsplit:'@#$',ID-from:'/user/demo/test',OD-targetlog:'/home/demo/data-clean.log',OD-target:'/user/demo/result'}";
        parameter = "{\"DC-inputsplit\":\"@#$\",\"DC-datadic\":\"2\"}";
        parameter = "{\"DC-inputsplit\":\"@#$\",\"DC-startcmd\":\"/opt/branbook/bigdata-core/xmonitor.sh\",\"DC-datadic\":\"020\",\"OD-targetlog\":\"/home/demo/clean.log\",\"DC-outputsplit\":\"@#$\",\"OD-target\":\"/user/demo/result\",\"ID-from\":\"/user/demo/wap\"}";
        final Map<String, String> params = ParameterParser.convertJson2Map(parameter);
        final Set<String> key = params.keySet();
        for (final String field : key) {
            System.out.println(field + ":" + params.get(field));
        }
    }
}
