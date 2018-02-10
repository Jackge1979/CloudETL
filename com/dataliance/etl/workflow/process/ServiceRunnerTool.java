package com.dataliance.etl.workflow.process;

import org.apache.hadoop.conf.*;

import com.dataliance.etl.workflow.driver.*;

import java.util.*;

public class ServiceRunnerTool
{
    public static int execute(Configuration conf, final IServiceJob serviceJob, final Map<String, String> jobInfo) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        conf.set("jobParameter", SerializationUtil.serializeObject(jobInfo));
        serviceJob.setConf(conf);
        serviceJob.setJobInfo(jobInfo);
        return serviceJob.run();
    }
}
