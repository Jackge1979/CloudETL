package com.dataliance.etl.workflow.process;

import java.util.*;

import com.dataliance.etl.workflow.bean.*;

public class JobUtils
{
    public static Map<Integer, ProcessField> convertParseOrderMap(final ProcessDictionary processDictionary) {
        final Map<Integer, ProcessField> parseOrder2Fields = new HashMap<Integer, ProcessField>();
        final List<ProcessField> fields = processDictionary.getProcessFields();
        for (final ProcessField field : fields) {
            if (field.getEnable()) {
                parseOrder2Fields.put(field.getParseOrder(), field);
            }
        }
        return parseOrder2Fields;
    }
}
