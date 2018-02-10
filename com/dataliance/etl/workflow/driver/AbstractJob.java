package com.dataliance.etl.workflow.driver;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.etl.workflow.bean.*;
import com.dataliance.etl.workflow.dao.*;
import com.dataliance.etl.workflow.process.*;

import org.apache.commons.cli.*;
import java.io.*;
import org.apache.commons.logging.*;

public abstract class AbstractJob extends Configured implements IServiceJob
{
    protected static final Log LOG;
    protected IProcessDictionaryDao dbBaseDao;
    protected ProcessDictionary processDictionary;
    protected Map<String, String> jobInfo;
    
    AbstractJob() {
        this.dbBaseDao = null;
        this.processDictionary = null;
        this.jobInfo = null;
        this.dbBaseDao = new DBBasedProcessDictionary();
    }
    
    public void setJobInfo(final Map<String, String> jobInfo) {
        this.jobInfo = jobInfo;
    }
    
    public Map<String, String> getJobInfo() {
        return this.jobInfo;
    }
    
    public int run() throws Exception {
        int exitCode = -1;
        if (!this.checkMustOption(this.getJobInfo())) {
            return exitCode;
        }
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        final Job job = this.createSubmittableJob(this.getJobInfo());
        final Configuration conf = this.getConf();
        if (conf.getBoolean("mr.job.monitor.enable", false)) {
            final String taskName = this.jobInfo.get(ETLConstants.JOB_MONTIOR.taskName.toString());
            final int index = Integer.parseInt(this.jobInfo.get(ETLConstants.JOB_MONTIOR.currentTaskIndex.toString()));
            final int total = Integer.parseInt(this.jobInfo.get(ETLConstants.JOB_MONTIOR.taskCount.toString()));
            final JobMontior montior = new JobMontior(this.jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString()), job, taskName, index, total);
            montior.start();
            try {
                exitCode = (job.waitForCompletion(true) ? 0 : 1);
                if (exitCode == 1) {
                    montior.stop(JobInfo.STATUS.ERROR);
                }
            }
            catch (Exception e) {
                montior.stop(JobInfo.STATUS.ERROR);
                throw new Exception(e.getMessage());
            }
            montior.stop();
        }
        else {
            exitCode = (job.waitForCompletion(true) ? 0 : 1);
        }
        final Counter validCounter = job.getCounters().findCounter(JobConstants.GROUP_NAME.VALID_COUNT.name(), JobConstants.ROW_COUNTER.ROWS.name());
        if (exitCode == 0 && validCounter.getValue() > 0L) {
            this.saveDictionaryInfo(this.getJobInfo());
        }
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        AbstractJob.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        AbstractJob.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public boolean checkMustOption(final Map<String, String> jobInfo) throws Exception {
        final Options options = this.buildOptions();
        if (!jobInfo.containsKey(ETLConstants.INPUT_DATA.ID_from.toString())) {
            this.printUsage(options);
            return false;
        }
        if (!jobInfo.containsKey(ETLConstants.OUTPUT_DATA.OD_target.toString())) {
            this.printUsage(options);
            return false;
        }
        return this.checkDictionaryData();
    }
    
    public abstract boolean checkDictionaryData() throws Exception;
    
    public abstract void saveDictionaryInfo(final Map<String, String> p0);
    
    public Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] input directory of data");
        options.addOption("output", true, "[must] target directory of data");
        return options;
    }
    
    public void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp(this.getJobNameForHelper(), options);
    }
    
    public abstract Job createSubmittableJob(final Map<String, String> p0) throws IOException;
    
    public abstract String getJobNameForHelper();
    
    static {
        LOG = LogFactory.getLog((Class)AbstractJob.class);
    }
}
