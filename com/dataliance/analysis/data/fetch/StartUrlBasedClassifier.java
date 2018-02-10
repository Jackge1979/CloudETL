package com.dataliance.analysis.data.fetch;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;

import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;
import com.dataliance.analysis.data.mapreduce.*;

import java.util.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.mapreduce.*;

public class StartUrlBasedClassifier extends AbstractJsonParse implements Tool
{
    public Configuration conf;
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = AbstractJsonParse.getConfig();
        ToolRunner.run(conf, (Tool)new StartUrlBasedClassifier(), args);
    }
    
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
    
    public Configuration getConf() {
        return this.conf;
    }
    
    public int run(final String[] args) throws Exception {
        final Map<String, String> jobInfo = AbstractJsonParse.parseArgs(args);
        final String programId = AbstractJsonParse.getProgramId(jobInfo);
        if (StringUtil.isEmpty(programId)) {
            throw new IOException("The params why key = " + ETLConstants.PROGRAM_ID.PD_id.toString() + " is not exist");
        }
        final String taskName = AbstractJsonParse.getTaskName(jobInfo);
        final int index = AbstractJsonParse.getIndex(jobInfo);
        final int total = AbstractJsonParse.getTotal(jobInfo);
        final String inDir = AbstractJsonParse.getInDir(jobInfo);
        final String outDir = AbstractJsonParse.getOutDir(jobInfo);
        final ArrayList<String> params = new ArrayList<String>();
        params.add("-input");
        params.add(inDir);
        params.add("-output");
        params.add(outDir);
        params.add("-fieldindex");
        params.add("4");
        final UrlBasedClassifier urlBase = new UrlBasedClassifier();
        urlBase.setConf(this.getConf());
        final Options options = UrlBasedClassifier.buildOptions();
        final BasicParser parser = new BasicParser();
        final CommandLine commands = parser.parse(options, (String[])params.toArray(new String[params.size()]));
        if (!commands.hasOption("input")) {
            UrlBasedClassifier.printUsage(options);
            return -1;
        }
        if (!commands.hasOption("output")) {
            UrlBasedClassifier.printUsage(options);
            return -1;
        }
        if (!commands.hasOption("fieldindex")) {
            UrlBasedClassifier.printUsage(options);
            return -1;
        }
        final Job job = urlBase.createSubmittableJob(commands);
        final JobMontior jobMontior = new JobMontior(programId, job, taskName, index, total);
        jobMontior.start();
        try {
            final int exitCode = job.waitForCompletion(true) ? 0 : 1;
            jobMontior.stop();
            return exitCode;
        }
        catch (Exception e) {
            jobMontior.stop(JobInfo.STATUS.ERROR);
            return -1;
        }
    }
}
