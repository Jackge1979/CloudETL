package com.dataliance.etl.workflow.process;

public class ETLConstants
{
    public static final String JOB_PARAMETER_KEY = "jobParameter";
    public static final String JOB_DATA_DIC_KEY = "jobDataDic";
    
    public enum INPUT_DATA
    {
        ID_from;
    }
    
    public enum PROGRAM_ID
    {
        PD_id;
    }
    
    public enum OUTPUT_DATA
    {
        OD_target, 
        OD_targetlog;
    }
    
    public enum DATA_CLEAN
    {
        DC_startcmd, 
        DC_datadic, 
        DC_inputsplit, 
        DC_outputsplit, 
        DC_desc, 
        DC_taskpool, 
        DC_reducenum;
    }
    
    public enum DATA_TRANSFER
    {
        DT_startcmd, 
        DT_datadic, 
        DT_inputsplit, 
        DT_outputsplit, 
        DT_desc, 
        DT_taskpool, 
        DT_reducenum;
    }
    
    public enum DATA_LOAD
    {
        DL_startcmd;
    }
    
    public enum JOB_MONTIOR
    {
        taskCount, 
        currentTaskIndex, 
        taskName;
    }
    
    public enum FUNCTION
    {
        dateformat, 
        replace, 
        sum, 
        split;
    }
}
