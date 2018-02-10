package com.dataliance.activemq.service.core;

import java.util.*;

public class JobInfo
{
    public String serviceName;
    public Map<String, String> jobArgs;
    public int jobType;
    public String jobId;
    
    public JobInfo() {
        this.serviceName = "unknow";
        this.jobType = -1;
    }
    
    @Override
    public String toString() {
        final StringBuffer jobArgsBuffer = new StringBuffer();
        for (final Map.Entry<String, String> entry : this.jobArgs.entrySet()) {
            jobArgsBuffer.append(String.format("%s=%s,", entry.getKey(), entry.getValue()));
        }
        return String.format("[job_type:'%s', job_id:'%s', args:'%s']", (this.jobType == 1) ? "interactive" : "realtime", this.jobId, jobArgsBuffer.toString());
    }
    
    public enum ControlCommand
    {
        STOP("stop"), 
        START("start"), 
        UNKOWN("null");
        
        private String name;
        
        private ControlCommand(final String name) {
            this.name = name;
        }
        
        public static ControlCommand parse(final String command) {
            for (final ControlCommand controlCommand : values()) {
                if (controlCommand.name.equalsIgnoreCase(command) || controlCommand.name().equalsIgnoreCase(command)) {
                    return controlCommand;
                }
            }
            return ControlCommand.UNKOWN;
        }
    }
}
