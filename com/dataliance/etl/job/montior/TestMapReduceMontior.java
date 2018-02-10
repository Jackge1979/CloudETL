package com.dataliance.etl.job.montior;

import java.io.*;
import java.util.*;

import com.dataliance.etl.job.vo.*;

public class TestMapReduceMontior implements MapReduceMontior
{
    @Override
    public JobInfo.JOB_TYPE getType() {
        return JobInfo.JOB_TYPE.MAPREDUCE;
    }
    
    @Override
    public MapReduceMontior toMapReduceMontior() throws IOException {
        return this;
    }
    
    @Override
    public ProgramMonitor toProgramMonitor() throws IOException {
        throw TestMapReduceMontior.NOT_SUPPORT;
    }
    
    @Override
    public MapRedeceJob getJobStatus() throws IOException {
        final MapRedeceJob map = new MapRedeceJob();
        map.setFinish(false);
        map.setJobid("JOB_11100_" + new Random().nextInt(100));
        map.setJobName("Test_" + new Random().nextInt(100));
        map.setMapComplete(new Random().nextFloat());
        map.setReduceComplete(new Random().nextFloat());
        map.setStartTime(System.currentTimeMillis());
        map.setState(MapRedeceJob.State.RUNNING);
        map.setUser("demo");
        return map;
    }
    
    @Override
    public boolean isFinish() {
        return false;
    }
    
    @Override
    public JobInfo.STATUS getStatus() {
        return JobInfo.STATUS.RUNNING;
    }
    
    @Override
    public String getJobName() {
        try {
            return this.getJobStatus().getJobName();
        }
        catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }
}
