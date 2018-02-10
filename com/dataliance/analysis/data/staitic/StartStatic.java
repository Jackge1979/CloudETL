package com.dataliance.analysis.data.staitic;

import java.io.*;
import com.dataliance.service.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import com.dataliance.analysis.data.region.mapreduce.*;
import com.dataliance.analysis.data.terminal.mapreduce.*;
import com.dataliance.analysis.data.uv.mapreduce.*;
import com.dataliance.analysis.data.pv.mapreduce.*;
import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.util.*;
import com.dataliance.analysis.data.flow.mapreduce.*;
import org.apache.hadoop.conf.*;
import java.util.*;

public class StartStatic
{
    public static void main(final String[] args) throws Exception {
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
        final String programId = jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString());
        final String taskName = jobInfo.get(ETLConstants.JOB_MONTIOR.taskName.toString());
        final int index = Integer.parseInt(jobInfo.get(ETLConstants.JOB_MONTIOR.currentTaskIndex.toString()));
        final int total = Integer.parseInt(jobInfo.get(ETLConstants.JOB_MONTIOR.taskCount.toString()));
        if (StringUtil.isEmpty(programId)) {
            throw new IOException("The params why key = " + ETLConstants.PROGRAM_ID.PD_id.toString() + " is not exist");
        }
        final Configuration conf = ConfigUtils.getConfig();
        final String inDir = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final JobMontior jobMontior = new JobMontior(programId, new Job(conf), taskName, index, total);
        conf.set("mapred.reduce.tasks", "30");
        jobMontior.start();
        final Path path = new Path(inDir);
        final String data = path.getName();
        String out = createOutput("usrPortrait/regionDistribution/", data, "/filter");
        ToolRunner.run(conf, (Tool)new RegionDistributionFilterByDate(), getParams(inDir, out));
        String inTmp = out;
        out = createOutput("usrPortrait/regionDistribution/", data, "/count");
        ToolRunner.run(conf, (Tool)new RegionDistributionCountByDate(), getParams(inTmp, out));
        out = createOutput("usrPortrait/terminalSort/", data, "/count");
        ToolRunner.run(conf, (Tool)new TerminalCountByDate(), getParams(inDir, out));
        inTmp = out;
        out = createOutput("usrPortrait/terminalSort/", data, "/sort");
        ToolRunner.run(conf, (Tool)new TerminalSortByDate(), getParams(inTmp, out));
        out = createOutput("usrPortrait/pv/", data, "");
        ToolRunner.run(conf, (Tool)new PVCountByDate(), getParams(inDir, out));
        out = createOutput("usrPortrait/uv/", data, "/filter");
        ToolRunner.run(conf, (Tool)new UVFilterByDate(), getParams(inDir, out));
        inTmp = out;
        out = createOutput("usrPortrait/uv/", data, "/count");
        ToolRunner.run(conf, (Tool)new UVCountByDate(), getParams(inTmp, out));
        out = createOutput("usrPortrait/flow/", data, "");
        ToolRunner.run(conf, (Tool)new FlowCountByDate(), getParams(inDir, out));
        jobMontior.stop();
    }
    
    private static String[] getParams(final String input, final String output) {
        final ArrayList<String> params = new ArrayList<String>();
        params.add("-input");
        params.add(input);
        params.add("-output");
        params.add(output);
        return params.toArray(new String[params.size()]);
    }
    
    private static String createOutput(final String prefix, final String data, final String suffix) {
        return prefix + data + suffix;
    }
}
