package com.dataliance.etl.job.montior;

import java.util.*;

import com.dataliance.etl.inject.job.vo.*;

public interface ProgramMonitor extends Montior
{
    List<Host> getAllDataHosts();
    
    List<Host> getAllImportHosts();
    
    List<Data> getAllDatas();
    
    List<Data> getAllFinishDatas();
    
    List<Data> getAllWaitDatas();
    
    List<Data> getAllRunDatas();
    
    List<Data> getAllErrorDatas();
    
    Host getManagerHost();
    
    float getComplete();
}
