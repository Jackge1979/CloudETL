package com.dataliance.etl.workflow.dao;

import java.util.*;

import com.dataliance.etl.workflow.bean.*;

import java.io.*;
import java.sql.*;

public interface IProcessDictionaryDao
{
    ProcessDictionary getProcessDictionaryById(final int p0) throws Exception;
    
    List<ProcessDictionary> getAllProcessDictionaries() throws Exception;
    
    List<ProcessDictionary> getProcessDictionariesByRegionId(final int p0) throws Exception;
    
    ProcessDictionary getDictionaryByRegionIdAndWorkDir(final String p0, final String p1) throws Exception;
    
    boolean deleteProcessDictionaryById(final int p0) throws Exception;
    
    boolean deleteProcessDictionaryByRegionId(final int p0) throws Exception;
    
    boolean deteteAll() throws Exception;
    
    void updateDataDifinition(final ProcessDictionary p0) throws Exception;
    
    long saveProcessDictionary(final ProcessDictionary p0) throws IOException;
    
    Object saveProcessFieldBatch(final List<ProcessField> p0) throws IOException, SQLException;
    
    long saveProcessField(final ProcessField p0) throws IOException, SQLException;
}
