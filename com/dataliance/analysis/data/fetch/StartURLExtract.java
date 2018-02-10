package com.dataliance.analysis.data.fetch;

import java.io.*;
import com.dataliance.log.url.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;

import java.util.*;
import org.apache.hadoop.conf.*;

public class StartURLExtract extends AbstractJsonParse
{
    public static void main(final String[] args) throws Exception {
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
        final Configuration conf = AbstractJsonParse.getConfig();
        final ArrayList<String> params = new ArrayList<String>();
        params.add(inDir);
        params.add(outDir);
        final ExtractURL extractURL = new ExtractURL();
        extractURL.setConf(conf);
        extractURL.initJob();
        final JobMontior jobMontior = new JobMontior(programId, extractURL.getJob(), taskName, index, total);
        jobMontior.start();
        extractURL.run(params.toArray(new String[params.size()]));
        jobMontior.stop();
    }
}
