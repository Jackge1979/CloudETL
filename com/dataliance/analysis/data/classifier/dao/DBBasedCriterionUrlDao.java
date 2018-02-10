package com.dataliance.analysis.data.classifier.dao;

import com.dataliance.analysis.data.classifier.bean.*;
import com.dataliance.service.util.*;
import java.util.*;

public class DBBasedCriterionUrlDao implements ICriterionUrlDao
{
    @Override
    public void saveCriterionUrl(final CriterionUrl criterionUrl) throws Exception {
        IbatisSqlMapClient.get().insert("Classifier.saveCriterionUrl", (Object)criterionUrl);
    }
    
    @Override
    public List<CriterionUrl> getAllCriterionUrls() throws Exception {
        List<CriterionUrl> criterionUrls = new ArrayList<CriterionUrl>();
        criterionUrls = (List<CriterionUrl>)IbatisSqlMapClient.get().queryForList("Classifier.getAllCriterionUrls");
        return criterionUrls;
    }
    
    @Override
    public Map<String, Integer> getAllUrlCategories() throws Exception {
        List<CriterionUrl> criterionUrls = new ArrayList<CriterionUrl>();
        criterionUrls = (List<CriterionUrl>)IbatisSqlMapClient.get().queryForList("Classifier.getAllCriterionUrls");
        final Map<String, Integer> url2categoryId = new HashMap<String, Integer>();
        for (final CriterionUrl criterionUrl : criterionUrls) {
            url2categoryId.put(criterionUrl.getUrl(), criterionUrl.getCategoryId());
        }
        return url2categoryId;
    }
    
    @Override
    public CriterionUrl getCriterionUrlById(final String id) throws Exception {
        CriterionUrl criterionUrl = new CriterionUrl();
        criterionUrl = (CriterionUrl)IbatisSqlMapClient.get().queryForObject("Classifier.getCriterionUrlById", (Object)id);
        return criterionUrl;
    }
    
    @Override
    public CriterionUrl getCriterionUrlByUrl(final String url) throws Exception {
        CriterionUrl criterionUrl = new CriterionUrl();
        criterionUrl = (CriterionUrl)IbatisSqlMapClient.get().queryForObject("Classifier.getCriterionUrlByUrl", (Object)url);
        return criterionUrl;
    }
    
    @Override
    public void updateCriterionUrl(final CriterionUrl criterionUrl) throws Exception {
        throw new RuntimeException("not implement method!");
    }
    
    @Override
    public void deleteCriterionUrlById(final String id) throws Exception {
        IbatisSqlMapClient.get().delete("Classifier.deleteCriterionUrlById", (Object)id);
    }
    
    @Override
    public void deleteAll() throws Exception {
        IbatisSqlMapClient.get().delete("Classifier.deleteAll");
    }
    
    public static void printForDebug() throws Exception {
        final ICriterionUrlDao criterionUrlDao = new DBBasedCriterionUrlDao();
        final List<CriterionUrl> criterionUrls = criterionUrlDao.getAllCriterionUrls();
        for (final CriterionUrl url : criterionUrls) {
            System.out.println(url);
        }
    }
    
    @Override
    public List<CriterionUrl> getCriterionUrlByCategoryId(final String categoryId) throws Exception {
        List<CriterionUrl> criterionUrls = new ArrayList<CriterionUrl>();
        criterionUrls = (List<CriterionUrl>)IbatisSqlMapClient.get().queryForList("Classifier.getCriterionUrlByCategoryId", (Object)categoryId);
        return criterionUrls;
    }
    
    @Override
    public Map<Integer, List<CriterionUrl>> getAllCriterionUrlsForMap() throws Exception {
        final List<CriterionUrl> allCriterionUrls = this.getAllCriterionUrls();
        final Map<Integer, List<CriterionUrl>> categoryId2urlPatterns = new HashMap<Integer, List<CriterionUrl>>();
        List<CriterionUrl> criterionUrls = null;
        for (final CriterionUrl criterionUrl : allCriterionUrls) {
            if (categoryId2urlPatterns.containsKey(criterionUrl.getCategoryId())) {
                criterionUrls = categoryId2urlPatterns.get(criterionUrl.getCategoryId());
                criterionUrls.add(criterionUrl);
            }
            else {
                criterionUrls = new ArrayList<CriterionUrl>();
                criterionUrls.add(criterionUrl);
                categoryId2urlPatterns.put(criterionUrl.getCategoryId(), criterionUrls);
            }
        }
        return categoryId2urlPatterns;
    }
    
    public static void main(final String[] args) throws Exception {
        final DBBasedCriterionUrlDao dao = new DBBasedCriterionUrlDao();
        dao.getAllCriterionUrlsForMap();
    }
}
