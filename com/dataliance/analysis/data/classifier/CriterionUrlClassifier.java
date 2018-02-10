package com.dataliance.analysis.data.classifier;

import com.dataliance.analysis.data.classifier.bean.*;
import com.dataliance.analysis.data.classifier.dao.*;
import com.dataliance.cloud.common.match.*;
import com.dataliance.cloud.common.match.generate.*;

import java.util.*;
import java.io.*;

import org.slf4j.*;

public class CriterionUrlClassifier
{
    private static final Logger LOG;
    private Map<Integer, List<CriterionUrl>> categoryId2UrlPatterns;
    private RuleEngine ruleEngine;
    
    public CriterionUrlClassifier(final Map<Integer, List<CriterionUrl>> categoryId2UrlPatterns) {
        this.categoryId2UrlPatterns = new HashMap<Integer, List<CriterionUrl>>();
        this.ruleEngine = null;
        this.categoryId2UrlPatterns = categoryId2UrlPatterns;
        this.initRuleEngine();
    }
    
    private void initRuleEngine() {
        final String ruleContent = this.generateAcceptRuleForAllCategories(this.categoryId2UrlPatterns);
        (this.ruleEngine = new RuleEngine()).setRuleContent(ruleContent);
    }
    
    public int doClassify(final String url) {
        int categoryLabel = -1;
        final MatchResult matchResult = this.ruleEngine.match(url);
        if (matchResult.containMatchRules()) {
            final List<MatchRule> matchRules = (List<MatchRule>)matchResult.acceptMatchRules;
            int clausesCnt = 0;
            MatchRule maxClauseMatchRule = null;
            for (final MatchRule matchRule : matchRules) {
                if (matchRule.clauses.size() > clausesCnt) {
                    clausesCnt = matchRule.clauses.size();
                    maxClauseMatchRule = matchRule;
                }
            }
            categoryLabel = this.extractClassIdFromMatchRule(maxClauseMatchRule);
        }
        return categoryLabel;
    }
    
    protected Integer extractClassIdFromMatchRule(final MatchRule matchRule) {
        final Map<String, String> infoMap = this.parseExtraInfo(matchRule.extraInfo);
        try {
            return Integer.valueOf(infoMap.get("classId"));
        }
        catch (Exception e) {
            return -1;
        }
    }
    
    private Map<String, String> parseExtraInfo(final String extraInfo) {
        final Map<String, String> name2value = new HashMap<String, String>();
        for (final String pair : extraInfo.split("&")) {
            final String[] items = pair.split("=");
            name2value.put(items[0], items[1].trim());
        }
        return name2value;
    }
    
    private String generateAcceptRuleForAllCategories(final Map<Integer, List<CriterionUrl>> categoryId2UrlPatterns) {
        final RuleDocument ruleDoc = new RuleDocument();
        for (final Map.Entry<Integer, List<CriterionUrl>> entry : categoryId2UrlPatterns.entrySet()) {
            final int categoryId = entry.getKey();
            final List<CriterionUrl> criterionUrls = entry.getValue();
            if (null != criterionUrls) {
                if (criterionUrls.size() == 0) {
                    continue;
                }
                final RuleSet ruleSet = ruleDoc.getRuleSet(String.format("categoryId_%s", categoryId));
                for (final CriterionUrl criterionUrl : criterionUrls) {
                    final Variable varUrl = ruleSet.getVariableById(String.format("url_id_%s", criterionUrl.getId()));
                    final Rule rule = ruleSet.createRuleWithExtraInfo(String.format("classId=%s&distance=%s", categoryId, criterionUrl.getWordSpace()));
                    varUrl.addValue(criterionUrl.getUrl());
                    rule.addVariable(varUrl);
                    if (criterionUrl.getPattern() != null && !"".equals(criterionUrl.getPattern())) {
                        final Variable varPattern = ruleSet.getVariableById(String.format("pattern_id_%s", criterionUrl.getId()));
                        varPattern.addValue(criterionUrl.getPattern());
                        rule.addVariable(varPattern);
                    }
                }
            }
        }
        return ruleDoc.toString();
    }
    
    private void generateRuleFile(final String rules) {
        try {
            final PrintWriter patternWriter = new PrintWriter(new FileWriter("./rules.txt"));
            patternWriter.println(rules);
            patternWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(final String[] args) throws Exception {
        final ICriterionUrlDao criterionUrlDao = new DBBasedCriterionUrlDao();
        final Map<Integer, List<CriterionUrl>> categoryId2urlPattern = criterionUrlDao.getAllCriterionUrlsForMap();
        final CriterionUrlClassifier criterionUrlClassifier = new CriterionUrlClassifier(categoryId2urlPattern);
        String url = "http://mail.sina.com.cn";
        url = "http://mail.sina.com.cn/index.html";
        url = "http://sina.com.cn/sports/index.html";
        url = "http://news.sina.cn/?sa=t141d48v68&pos=9&vt=4";
        url = "http://wap.sohu.com/news/china";
        System.out.println(criterionUrlClassifier.doClassify(url));
        criterionUrlClassifier.generateRuleFile(criterionUrlClassifier.generateAcceptRuleForAllCategories(categoryId2urlPattern));
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)CriterionUrlClassifier.class);
    }
}
