package com.dataliance.etl.workflow.driver;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.*;

import com.dataliance.etl.workflow.bean.*;
import com.dataliance.etl.workflow.mapper.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.service.util.*;

public class DataCleanJob extends AbstractJob
{
    @Override
    public Job createSubmittableJob(final Map<String, String> jobInfo) throws IOException {
        final String commaSeparatedPaths = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String output = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        DataCleanJob.LOG.info((Object)("inputPath:" + commaSeparatedPaths.toString()));
        DataCleanJob.LOG.info((Object)("outputPath:" + output));
        final Configuration conf = this.getConf();
        conf.set("jobDataDic", SerializationUtil.serializeObject(this.processDictionary));
        final Job job = new Job(conf, DataCleanJob.class.getSimpleName());
        job.setJarByClass((Class)DataCleanJob.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)DataCleanMapper.class);
        job.setOutputKeyClass((Class)NullWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        if (jobInfo.get(ETLConstants.DATA_CLEAN.DC_taskpool.toString()) != null) {
            conf.set("pool.name", (String)jobInfo.get(ETLConstants.DATA_CLEAN.DC_taskpool.toString()));
        }
        int reduceNumber = 0;
        if (jobInfo.get(ETLConstants.DATA_CLEAN.DC_reducenum.toString()) != null) {
            reduceNumber = Integer.parseInt(jobInfo.get(ETLConstants.DATA_CLEAN.DC_reducenum.toString()));
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
        final String regionId = this.jobInfo.get(ETLConstants.DATA_CLEAN.DC_datadic.toString());
        this.processDictionary = this.dbBaseDao.getDictionaryByRegionIdAndWorkDir(regionId, workDir);
        if (this.processDictionary == null || this.processDictionary.getProcessFields().size() < 0) {
            DataCleanJob.LOG.info((Object)"please configure data dictionary for source directory");
            return false;
        }
        return true;
    }
    
    @Override
    public void saveDictionaryInfo(final Map<String, String> jobInfo) {
        final String output = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        try {
            final ProcessDictionary storeProcessDictionary = new ProcessDictionary();
            storeProcessDictionary.setName(this.processDictionary.getName() + "(\u6e05\u6d17)");
            storeProcessDictionary.setRegionId(this.processDictionary.getRegionId());
            storeProcessDictionary.setWorkDir(output);
            storeProcessDictionary.setSplitLen(this.processDictionary.getSplitLen());
            storeProcessDictionary.setParseSplitSymbol(this.processDictionary.getDestSplitSymbol());
            storeProcessDictionary.setDestSplitSymbol(this.processDictionary.getDestSplitSymbol());
            storeProcessDictionary.setDescription(jobInfo.get(ETLConstants.DATA_CLEAN.DC_desc.toString()));
            final List<ProcessField> processFields = new ArrayList<ProcessField>();
            ProcessField storeProcessField = null;
            for (final ProcessField field : this.processDictionary.getProcessFields()) {
                storeProcessField = field.clone();
                storeProcessField.setParseOrder(field.getDestOrder());
                processFields.add(storeProcessField);
            }
            storeProcessDictionary.setProcessFields(processFields);
            this.dbBaseDao.saveProcessDictionary(storeProcessDictionary);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", DataCleanJob.class.getSimpleName()));
            System.exit(1);
        }
        Map<String, String> jobInfo = new HashMap<String, String>();
        try {
            jobInfo = ParameterParser.convertJson2Map(args[0]);
        }
        catch (Exception e) {
            System.out.println("arg: " + args[0]);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", DataCleanJob.class.getSimpleName()));
            e.printStackTrace();
            System.exit(1);
        }
        final Configuration conf = ConfigUtils.getConfig();
        final int exitCode = ServiceRunnerTool.execute(conf, new DataCleanJob(), jobInfo);
        System.exit(exitCode);
    }
}
