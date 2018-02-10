package com.dataliance.analysis.data.fetch;

import java.io.*;
import org.apache.hadoop.mapreduce.*;

import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;

import java.util.*;

public class StartCrawl extends AbstractJsonParse
{
    public static void main(final String[] args) throws IOException {
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
        final JobMontior jobMontior = new JobMontior(programId, new Job(), taskName, index, total);
        jobMontior.start();
        System.out.println("Can not crawl segment " + inDir);
        jobMontior.stop();
        System.exit(0);
    }
}
