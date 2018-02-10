package com.dataliance.analysis.data.classifier.dao;

import java.util.*;

import com.dataliance.analysis.data.classifier.bean.*;

public class MemoryBasedCriterionUrlDao implements ICriterionUrlDao
{
    private Map<String, CriterionUrl> id2criterionUrl;
    private Map<String, CriterionUrl> url2criterionUrl;
    private List<CriterionUrl> criterionUrls;
    
    public MemoryBasedCriterionUrlDao() {
        this.id2criterionUrl = new HashMap<String, CriterionUrl>();
        this.url2criterionUrl = new HashMap<String, CriterionUrl>();
        this.criterionUrls = new ArrayList<CriterionUrl>();
    }
    
    @Override
    public void saveCriterionUrl(final CriterionUrl criterionUrl) throws Exception {
        this.criterionUrls.add(criterionUrl);
        this.id2criterionUrl.put(criterionUrl.getId() + "", criterionUrl);
        this.url2criterionUrl.put(criterionUrl.getUrl(), criterionUrl);
    }
    
    @Override
    public List<CriterionUrl> getAllCriterionUrls() throws Exception {
        return this.criterionUrls;
    }
    
    @Override
    public Map<String, Integer> getAllUrlCategories() throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public CriterionUrl getCriterionUrlById(final String id) throws Exception {
        return this.id2criterionUrl.get(id);
    }
    
    @Override
    public CriterionUrl getCriterionUrlByUrl(final String url) throws Exception {
        return this.url2criterionUrl.get(url);
    }
    
    @Override
    public void updateCriterionUrl(final CriterionUrl criterionUrl) throws Exception {
        this.criterionUrls.add(criterionUrl);
        this.id2criterionUrl.put(criterionUrl.getId() + "", criterionUrl);
        this.url2criterionUrl.put(criterionUrl.getUrl(), criterionUrl);
    }
    
    @Override
    public void deleteCriterionUrlById(final String id) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public void deleteAll() throws Exception {
        this.criterionUrls.clear();
        this.id2criterionUrl.clear();
        this.url2criterionUrl.clear();
    }
    
    @Override
    public List<CriterionUrl> getCriterionUrlByCategoryId(final String categoryId) throws Exception {
        return null;
    }
    
    @Override
    public Map<Integer, List<CriterionUrl>> getAllCriterionUrlsForMap() throws Exception {
        return null;
    }
}
