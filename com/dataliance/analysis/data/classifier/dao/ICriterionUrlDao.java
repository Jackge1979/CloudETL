package com.dataliance.analysis.data.classifier.dao;

import java.util.*;

import com.dataliance.analysis.data.classifier.bean.*;

public interface ICriterionUrlDao
{
    void saveCriterionUrl(final CriterionUrl p0) throws Exception;
    
    List<CriterionUrl> getAllCriterionUrls() throws Exception;
    
    Map<String, Integer> getAllUrlCategories() throws Exception;
    
    CriterionUrl getCriterionUrlById(final String p0) throws Exception;
    
    CriterionUrl getCriterionUrlByUrl(final String p0) throws Exception;
    
    void updateCriterionUrl(final CriterionUrl p0) throws Exception;
    
    void deleteCriterionUrlById(final String p0) throws Exception;
    
    void deleteAll() throws Exception;
    
    List<CriterionUrl> getCriterionUrlByCategoryId(final String p0) throws Exception;
    
    Map<Integer, List<CriterionUrl>> getAllCriterionUrlsForMap() throws Exception;
}
