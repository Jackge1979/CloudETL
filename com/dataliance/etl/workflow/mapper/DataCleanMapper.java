package com.dataliance.etl.workflow.mapper;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.etl.workflow.bean.*;
import com.dataliance.etl.workflow.process.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.apache.commons.lang.*;
import org.apache.commons.logging.*;

public class DataCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private static final Log LOG;
    private Configuration conf;
    private ProcessDictionary processDictionary;
    private String inputSplit;
    private String outputSplit;
    private int feildCount;
    private Map<Integer, ProcessField> parseOrder2Fields;
    
    public DataCleanMapper() {
        this.conf = null;
        this.processDictionary = null;
        this.inputSplit = "";
        this.outputSplit = "";
        this.feildCount = 0;
        this.parseOrder2Fields = null;
    }
    
    public void setup(final Mapper.Context context) {
        this.conf = context.getConfiguration();
        try {
            this.parseOrder2Fields = new HashMap<Integer, ProcessField>();
            this.convertParseOrderMap(this.processDictionary = (ProcessDictionary)SerializationUtil.deserializeObject(this.conf.get("jobDataDic"), ProcessDictionary.class));
            this.feildCount = this.processDictionary.getSplitLen();
            this.inputSplit = this.processDictionary.getParseSplitSymbol();
            this.outputSplit = this.processDictionary.getDestSplitSymbol();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split(this.inputSplit);
        if (fields.length != this.feildCount) {
            return;
        }
        if (!this.verifyExpression(fields)) {
            return;
        }
        value.set(this.getOutputValue(fields));
        context.getCounter(JobConstants.GROUP_NAME.VALID_COUNT.name(), JobConstants.ROW_COUNTER.ROWS.name()).increment(1L);
        context.write((Object)NullWritable.get(), (Object)value);
    }
    
    private void convertParseOrderMap(final ProcessDictionary processDictionary) {
        final List<ProcessField> fields = processDictionary.getProcessFields();
        for (final ProcessField field : fields) {
            if (field.getEnable()) {
                this.parseOrder2Fields.put(field.getParseOrder(), field);
            }
        }
    }
    
    private boolean verifyExpression(final String[] fields) {
        final int size = fields.length;
        String fieldValue = null;
        ProcessField processField = null;
        for (int i = 0; i < size; ++i) {
            fieldValue = fields[i];
            processField = this.parseOrder2Fields.get(i);
            if (processField != null) {
                if (processField.getNeedVerify()) {
                    final boolean isMatch = Pattern.matches(processField.getRegularExpressions(), fieldValue);
                    DataCleanMapper.LOG.info((Object)("verifyExpression:" + processField.getRegularExpressions()));
                    if (!isMatch) {
                        DataCleanMapper.LOG.warn((Object)String.format("%s of value %s not matched by %s at verifyExpression phase", processField.getName(), fieldValue, processField.getRegularExpressions()));
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    private String getOutputValue(final String[] fields) {
        final int size = fields.length;
        String fieldValue = null;
        ProcessField processField = null;
        final String[] out = new String[size];
        for (int i = 0; i < size; ++i) {
            fieldValue = fields[i];
            processField = this.parseOrder2Fields.get(i);
            if (processField != null) {
                if (processField.getDestOrder() >= size) {
                    DataCleanMapper.LOG.warn((Object)String.format("%s not correct set destOrder %s ", processField.getName(), processField.getDestOrder()));
                }
                else {
                    out[processField.getDestOrder()] = fieldValue;
                }
            }
        }
        return StringUtils.join((Object[])out, this.outputSplit);
    }
    
    protected void cleanup(final Mapper.Context context) throws IOException, InterruptedException {
    }
    
    static {
        LOG = LogFactory.getLog((Class)DataCleanMapper.class);
    }
}
