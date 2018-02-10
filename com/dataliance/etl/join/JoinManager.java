package com.dataliance.etl.join;

import com.dataliance.service.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;

import java.io.*;
import java.util.*;

public class JoinManager extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = ConfigUtils.getConfig();
        ToolRunner.run(conf, (Tool)new JoinManager(), args);
    }
    
    public int run(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("args count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", JoinManager.class.getSimpleName()));
            System.exit(1);
            return 0;
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
        final String programId = jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString());
        if (StringUtil.isEmpty(programId)) {
            throw new IOException("The params why key = " + ETLConstants.PROGRAM_ID.PD_id.toString() + " is not exist");
        }
        final Configuration conf = this.getConf();
        return 0;
    }
}
