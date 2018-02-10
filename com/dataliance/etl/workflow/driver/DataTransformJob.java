package com.dataliance.etl.workflow.driver;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.regex.*;
import java.util.*;

import com.dataliance.etl.workflow.bean.*;
import com.dataliance.etl.workflow.mapper.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.service.util.*;

public class DataTransformJob extends AbstractJob
{
    @Override
    public Job createSubmittableJob(final Map<String, String> jobInfo) throws IOException {
        final String commaSeparatedPaths = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String output = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        DataTransformJob.LOG.info((Object)("inputPath:" + commaSeparatedPaths.toString()));
        DataTransformJob.LOG.info((Object)("outputPath:" + output));
        final Configuration conf = this.getConf();
        conf.set("jobDataDic", SerializationUtil.serializeObject(this.processDictionary));
        final Job job = new Job(conf, DataTransformJob.class.getSimpleName());
        job.setJarByClass((Class)DataTransformJob.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)DataTransformMapper.class);
        job.setOutputKeyClass((Class)NullWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        if (jobInfo.get(ETLConstants.DATA_TRANSFER.DT_taskpool.toString()) != null) {
            conf.set("pool.name", (String)jobInfo.get(ETLConstants.DATA_TRANSFER.DT_taskpool.toString()));
        }
        int reduceNumber = 0;
        if (jobInfo.get(ETLConstants.DATA_TRANSFER.DT_reducenum.toString()) != null) {
            reduceNumber = Integer.parseInt(jobInfo.get(ETLConstants.DATA_TRANSFER.DT_reducenum.toString()));
        }
        job.setNumReduceTasks(reduceNumber);
        return job;
    }
    
    @Override
    public String getJobNameForHelper() {
        return this.getClass().getSimpleName();
    }
    
    @Override
    public boolean checkDictionaryData() throws Exception {
        final String workDir = this.jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String regionId = this.jobInfo.get(ETLConstants.DATA_TRANSFER.DT_datadic.toString());
        this.processDictionary = this.dbBaseDao.getDictionaryByRegionIdAndWorkDir(regionId, workDir);
        if (this.processDictionary == null || this.processDictionary.getProcessFields().size() < 0) {
            DataTransformJob.LOG.info((Object)"please configure data dictionary for source directory");
            return false;
        }
        return true;
    }
    
    @Override
    public void saveDictionaryInfo(final Map<String, String> jobInfo) {
        final Map<Integer, ProcessField> parseOrder2Fields = JobUtils.convertParseOrderMap(this.processDictionary);
        final String output = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        try {
            final ProcessDictionary storeProcessDictionary = new ProcessDictionary();
            storeProcessDictionary.setName(this.processDictionary.getName() + "(\u6570\u636e\u8f6c\u6362)");
            storeProcessDictionary.setRegionId(this.processDictionary.getRegionId());
            storeProcessDictionary.setWorkDir(output);
            storeProcessDictionary.setSplitLen(this.processDictionary.getSplitLen());
            storeProcessDictionary.setParseSplitSymbol(this.processDictionary.getDestSplitSymbol());
            storeProcessDictionary.setDestSplitSymbol(this.processDictionary.getDestSplitSymbol());
            storeProcessDictionary.setDescription(jobInfo.get(ETLConstants.DATA_TRANSFER.DT_desc.toString()));
            final List<ProcessField> processFields = new ArrayList<ProcessField>();
            ProcessField storeProcessField = null;
            final List<ProcessField> addProcessFields = new ArrayList<ProcessField>();
            Collections.sort(this.processDictionary.getProcessFields(), new Comparator<ProcessField>() {
                @Override
                public int compare(final ProcessField o1, final ProcessField o2) {
                    return o1.getParseOrder() - o2.getParseOrder();
                }
            });
            for (final ProcessField field : this.processDictionary.getProcessFields()) {
                storeProcessField = field.clone();
                storeProcessField.setParseOrder(field.getDestOrder());
                processFields.add(storeProcessField);
                if (storeProcessField != null) {
                    if (!storeProcessField.getNeedTransfer()) {
                        continue;
                    }
                    final String destRule = storeProcessField.getDestRule();
                    final String[] type2rule = destRule.split(":", 2);
                    if (type2rule == null) {
                        continue;
                    }
                    if (type2rule.length != 2) {
                        continue;
                    }
                    if (ETLConstants.FUNCTION.sum.name().equalsIgnoreCase(type2rule[0])) {
                        this.getSumField(parseOrder2Fields, addProcessFields, field, type2rule);
                    }
                    else {
                        if (!ETLConstants.FUNCTION.split.name().equalsIgnoreCase(type2rule[0])) {
                            continue;
                        }
                        this.getSplitField(parseOrder2Fields, addProcessFields, field);
                    }
                }
            }
            processFields.addAll(addProcessFields);
            storeProcessDictionary.setProcessFields(processFields);
            this.dbBaseDao.saveProcessDictionary(storeProcessDictionary);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void getSplitField(final Map<Integer, ProcessField> parseOrder2Fields, final List<ProcessField> addProcessFields, final ProcessField field) {
        ProcessField addProcessField = parseOrder2Fields.get(field.getParseOrder()).clone();
        parseOrder2Fields.put(parseOrder2Fields.size(), addProcessField);
        addProcessField.setName(addProcessField.getName() + "-split1");
        addProcessField.setParseOrder(parseOrder2Fields.size() - 1);
        addProcessField.setDestRule("");
        addProcessField.setDestOrder(parseOrder2Fields.size() - 1);
        addProcessFields.add(addProcessField);
        addProcessField = parseOrder2Fields.get(field.getParseOrder()).clone();
        parseOrder2Fields.put(parseOrder2Fields.size(), addProcessField);
        addProcessField.setName(addProcessField.getName() + "-split2");
        addProcessField.setParseOrder(parseOrder2Fields.size() - 1);
        addProcessField.setDestRule("");
        addProcessField.setDestOrder(parseOrder2Fields.size() - 1);
        addProcessFields.add(addProcessField);
    }
    
    private void getSumField(final Map<Integer, ProcessField> parseOrder2Fields, final List<ProcessField> addProcessFields, final ProcessField field, final String[] type2rule) {
        final ProcessField addProcessField = parseOrder2Fields.get(field.getParseOrder()).clone();
        final String expression = type2rule[1];
        int start = 0;
        int index = 0;
        final Pattern pattern = Pattern.compile("(\\$(\\d+))");
        final Matcher matcher = pattern.matcher(expression);
        final StringBuilder buffer = new StringBuilder();
        while (matcher.find(start)) {
            final int count = matcher.groupCount();
            start = matcher.end();
            index = Integer.parseInt(matcher.group(count));
            buffer.append(parseOrder2Fields.get(index).getName());
            buffer.append("+");
        }
        parseOrder2Fields.put(parseOrder2Fields.size(), addProcessField);
        addProcessField.setName(buffer.toString().substring(0, buffer.toString().length() - 1));
        addProcessField.setParseOrder(parseOrder2Fields.size() - 1);
        addProcessField.setDestRule("");
        addProcessField.setDestOrder(parseOrder2Fields.size() - 1);
        addProcessFields.add(addProcessField);
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", DataTransformJob.class.getSimpleName()));
            System.exit(1);
        }
        Map<String, String> jobInfo = new HashMap<String, String>();
        try {
            jobInfo = ParameterParser.convertJson2Map(args[0]);
        }
        catch (Exception e) {
            System.out.println("arg: " + args[0]);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", DataTransformJob.class.getSimpleName()));
            e.printStackTrace();
            System.exit(1);
        }
        final Configuration conf = ConfigUtils.getConfig();
        final int exitCode = ServiceRunnerTool.execute(conf, new DataTransformJob(), jobInfo);
        System.exit(exitCode);
    }
}
