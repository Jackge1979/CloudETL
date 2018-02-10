package com.dataliance.etl.workflow.mapper;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import org.apache.log4j.lf5.util.*;

import com.dataliance.etl.workflow.bean.*;
import com.dataliance.etl.workflow.process.*;

import java.util.*;
import java.util.regex.*;
import org.apache.commons.lang.*;
import org.apache.commons.logging.*;

public class DataTransformMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
    private static final Log LOG;
    private Configuration conf;
    private ProcessDictionary processDictionary;
    private String inputSplit;
    private String outputSplit;
    private int feildCount;
    private Map<Integer, ProcessField> parseOrder2Fields;
    
    public DataTransformMapper() {
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
            this.processDictionary = (ProcessDictionary)SerializationUtil.deserializeObject(this.conf.get("jobDataDic"), ProcessDictionary.class);
            this.parseOrder2Fields = JobUtils.convertParseOrderMap(this.processDictionary);
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
        String[] transferFields = null;
        if (fields.length != this.feildCount) {
            return;
        }
        try {
            transferFields = this.doDataTransfer(fields);
        }
        catch (Exception e) {
            throw new InterruptedException(e.getMessage());
        }
        value.set(this.getOutputValue(transferFields));
        context.getCounter(JobConstants.GROUP_NAME.VALID_COUNT.name(), JobConstants.ROW_COUNTER.ROWS.name()).increment(1L);
        context.write((Object)NullWritable.get(), (Object)value);
    }
    
    private String[] doDataTransfer(final String[] fields) throws Exception {
        final int size = fields.length;
        String fieldValue = null;
        ProcessField processField = null;
        final List<String> produceResult = new ArrayList<String>();
        for (int i = 0; i < size; ++i) {
            fieldValue = fields[i];
            processField = this.parseOrder2Fields.get(i);
            if (processField != null) {
                if (processField.getNeedTransfer()) {
                    final String destRule = processField.getDestRule();
                    final String[] type2rule = destRule.split(":", 2);
                    if (type2rule != null) {
                        if (type2rule.length == 2) {
                            try {
                                if ("dateformat".equalsIgnoreCase(type2rule[0])) {
                                    final DateFormatManager dataFormatManager = new DateFormatManager();
                                    final Date date = dataFormatManager.parse(fieldValue, processField.getParseRule());
                                    fields[i] = dataFormatManager.format(date, "yyyy-MM-dd HH:mm:ss");
                                }
                                else if (ETLConstants.FUNCTION.replace.name().equalsIgnoreCase(type2rule[0])) {
                                    fields[i] = type2rule[1];
                                }
                                else if (ETLConstants.FUNCTION.sum.name().equalsIgnoreCase(type2rule[0])) {
                                    produceResult.add(this.getSumField(fields, i, type2rule[1]) + "");
                                }
                                else if (ETLConstants.FUNCTION.split.name().equalsIgnoreCase(type2rule[0])) {
                                    this.getSplitField(fieldValue, processField, produceResult, type2rule);
                                }
                            }
                            catch (Exception e) {
                                DataTransformMapper.LOG.warn((Object)String.format("%s of value %s from  %s to %s at transfer phase", processField.getName(), fieldValue, processField.getParseRule(), processField.getDestRule()));
                                throw new Exception(e.getMessage());
                            }
                        }
                    }
                }
            }
        }
        return (String[])ArrayUtils.addAll((Object[])fields, (Object[])produceResult.toArray(new String[0]));
    }
    
    private void getSplitField(final String fieldValue, final ProcessField processField, final List<String> produceResult, final String[] type2rule) {
        final String[] arr$;
        final String[] splitedValue = arr$ = fieldValue.split(type2rule[1], 2);
        for (final String value : arr$) {
            final int lastIndex = this.parseOrder2Fields.size();
            final ProcessField addProcessField = processField.clone();
            addProcessField.setDestOrder(lastIndex);
            this.parseOrder2Fields.put(lastIndex, addProcessField);
            produceResult.add(value);
        }
    }
    
    private Long getSumField(final String[] fields, final int fieldIndex, final String expression) {
        long sum = 0L;
        int start = 0;
        int index = 0;
        final Pattern pattern = Pattern.compile("(\\$(\\d+))");
        int count;
        for (Matcher matcher = pattern.matcher(expression); matcher.find(start); start = matcher.end(), index = Integer.parseInt(matcher.group(count)), sum += Long.parseLong(fields[index])) {
            count = matcher.groupCount();
        }
        final int lastIndex = this.parseOrder2Fields.size();
        final ProcessField processField = this.parseOrder2Fields.get(fieldIndex);
        final ProcessField addProcessField = processField.clone();
        addProcessField.setDestOrder(lastIndex);
        this.parseOrder2Fields.put(lastIndex, addProcessField);
        return sum;
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
                    DataTransformMapper.LOG.warn((Object)String.format("%s not correct set destOrder %s ", processField.getName(), processField.getDestOrder()));
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
        LOG = LogFactory.getLog((Class)DataTransformMapper.class);
    }
}
