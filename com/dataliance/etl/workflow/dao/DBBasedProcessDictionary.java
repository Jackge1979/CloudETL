package com.dataliance.etl.workflow.dao;

import com.dataliance.etl.workflow.bean.*;
import com.ibatis.sqlmap.client.*;

import java.sql.*;
import java.io.*;
import java.util.*;
import org.apache.commons.logging.*;
import com.dataliance.service.util.*;

public class DBBasedProcessDictionary implements IProcessDictionaryDao
{
    private static final Log LOG;
    private static SqlMapClient sqlMapClient;
    
    @Override
    public ProcessDictionary getProcessDictionaryById(final int id) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public List<ProcessDictionary> getAllProcessDictionaries() throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public List<ProcessDictionary> getProcessDictionariesByRegionId(final int RegionId) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public boolean deleteProcessDictionaryById(final int id) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public boolean deleteProcessDictionaryByRegionId(final int RegionId) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public boolean deteteAll() throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public void updateDataDifinition(final ProcessDictionary processDictionary) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public ProcessDictionary getDictionaryByRegionIdAndWorkDir(final String regionId, final String workDir) throws Exception {
        final Map<String, String> params = new HashMap<String, String>();
        params.put("regionId", regionId);
        params.put("workDir", workDir);
        ProcessDictionary dataDefinitions = new ProcessDictionary();
        dataDefinitions = (ProcessDictionary)DBBasedProcessDictionary.sqlMapClient.queryForObject("ETL.getDictionaryByRegionIdAndWorkDir", (Object)params);
        return dataDefinitions;
    }
    
    @Override
    public long saveProcessDictionary(final ProcessDictionary processDictionary) throws IOException {
        long primaryKey = 0L;
        try {
            DBBasedProcessDictionary.LOG.info((Object)"execute startTransaction...");
            DBBasedProcessDictionary.sqlMapClient.startTransaction();
            primaryKey = (long)DBBasedProcessDictionary.sqlMapClient.insert("ETL.saveProcessDictionary", (Object)processDictionary);
            DBBasedProcessDictionary.LOG.info((Object)("store processDictionary return primaryKey : " + primaryKey));
            for (final ProcessField field : processDictionary.getProcessFields()) {
                field.setDictionaryId(primaryKey);
            }
            this.saveProcessFieldBatch(processDictionary.getProcessFields());
            DBBasedProcessDictionary.sqlMapClient.commitTransaction();
            DBBasedProcessDictionary.LOG.info((Object)"execute commitTransaction...");
        }
        catch (Exception e) {
            DBBasedProcessDictionary.LOG.info((Object)"execute transaction...");
            e.printStackTrace();
            try {
                DBBasedProcessDictionary.sqlMapClient.endTransaction();
                DBBasedProcessDictionary.LOG.info((Object)"execute endTransaction...");
            }
            catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        finally {
            try {
                DBBasedProcessDictionary.sqlMapClient.endTransaction();
                DBBasedProcessDictionary.LOG.info((Object)"execute endTransaction...");
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
        return primaryKey;
    }
    
    @Override
    public Object saveProcessFieldBatch(final List<ProcessField> processFields) throws IOException, SQLException {
        return DBBasedProcessDictionary.sqlMapClient.insert("ETL.saveProcessFieldBatch", (Object)processFields);
    }
    
    @Override
    public long saveProcessField(final ProcessField processField) throws IOException, SQLException {
        final long primayKey = (long)DBBasedProcessDictionary.sqlMapClient.insert("ETL.saveProcessField", (Object)processField);
        DBBasedProcessDictionary.LOG.info((Object)("store processField return primaryKey : " + primayKey));
        return primayKey;
    }
    
    public static void main(final String[] args) throws Exception {
        final IProcessDictionaryDao dao = new DBBasedProcessDictionary();
        ProcessDictionary processDictionary = new ProcessDictionary();
        processDictionary = dao.getDictionaryByRegionIdAndWorkDir("19", "D:/testdata/test.txt");
        Collections.sort(processDictionary.getProcessFields(), new Comparator<ProcessField>() {
            @Override
            public int compare(final ProcessField o1, final ProcessField o2) {
                return o1.getParseOrder() - o2.getParseOrder();
            }
        });
        System.out.println(processDictionary);
    }
    
    static {
        LOG = LogFactory.getLog((Class)DBBasedProcessDictionary.class);
        DBBasedProcessDictionary.sqlMapClient = IbatisSqlMapClient.get();
    }
}
