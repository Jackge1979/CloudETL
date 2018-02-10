package com.dataliance.analysis.data.fetch;

import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;

import java.util.*;

import org.apache.hadoop.conf.*;
import com.dataliance.service.util.*;

public abstract class AbstractJsonParse
{
    public static Map<String, String> parseArgs(final String[] args) {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        Map<String, String> jobInfo = new HashMap<String, String>();
        try {
            jobInfo = ParameterParser.convertJson2Map(args[0]);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("arg: " + args[0]);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        return jobInfo;
    }
    
    public static String getProgramId(final Map<String, String> jobInfo) {
        return jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString());
    }
    
    public static String getTaskName(final Map<String, String> jobInfo) {
        return jobInfo.get(ETLConstants.JOB_MONTIOR.taskName.toString());
    }
    
    public static int getIndex(final Map<String, String> jobInfo) {
        return StringUtil.toInt(jobInfo.get(ETLConstants.JOB_MONTIOR.currentTaskIndex.toString()));
    }
    
    public static int getTotal(final Map<String, String> jobInfo) {
        return StringUtil.toInt(jobInfo.get(ETLConstants.JOB_MONTIOR.taskCount.toString()));
    }
    
    public static String getInDir(final Map<String, String> jobInfo) {
        return jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
    }
    
    public static String getOutDir(final Map<String, String> jobInfo) {
        return jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
    }
    
    public static Configuration getConfig() {
        return ConfigUtils.getConfig();
    }
}
