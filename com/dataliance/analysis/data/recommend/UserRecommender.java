package com.dataliance.analysis.data.recommend;

import org.apache.hadoop.conf.*;
import java.util.*;

public class UserRecommender
{
    UserBehaviorFeatureManager featureManager;
    AccessedFrequencyManager frequencyManager;
    
    public UserRecommender(final String mapFilePath, final String frequencyFilePath) {
        this.featureManager = null;
        this.frequencyManager = null;
        final Configuration conf = new Configuration();
        this.featureManager = new UserBehaviorFeatureManager(conf, mapFilePath);
        this.frequencyManager = new AccessedFrequencyManager(frequencyFilePath);
    }
    
    public UserRecommender(final Configuration conf, final String mapFilePath, final String frequencyFilePath) {
        this.featureManager = null;
        this.frequencyManager = null;
        this.featureManager = new UserBehaviorFeatureManager(conf, mapFilePath);
        this.frequencyManager = new AccessedFrequencyManager(frequencyFilePath);
    }
    
    public List<RecommendUserInfo> getRecommendedUser() {
        final List<AccessedFrequency> frequencys = this.frequencyManager.getAccessedFrequencys();
        final List<RecommendUserInfo> recommendUserInfos = new ArrayList<RecommendUserInfo>();
        RecommendUserInfo recommendUserInfo = null;
        for (final AccessedFrequency freq : frequencys) {
            recommendUserInfo = new RecommendUserInfo();
            final UserFeature userFeature = (UserFeature)this.featureManager.search(freq.getPhoneNumber() + "");
            if (userFeature == null) {
                continue;
            }
            recommendUserInfo.setPhoneNumber(freq.getPhoneNumber());
            recommendUserInfo.setFreq(freq.getFreq());
            recommendUserInfo.setFlow(userFeature.getFlow());
            recommendUserInfo.setRatType(userFeature.getRatType());
            recommendUserInfo.setStyle(userFeature.getStyle());
            if (this.isFilter(recommendUserInfo)) {
                continue;
            }
            recommendUserInfos.add(recommendUserInfo);
        }
        return recommendUserInfos;
    }
    
    private boolean isFilter(final RecommendUserInfo recommendUserInfo) {
        return "".equals(recommendUserInfo.getStyle());
    }
    
    public static void main(final String[] args) {
        final String mapFilePath = "E:\\git-repository\\git\\bigdata-core\\data\\UserFeature\\20120214\\part-r-00000";
        String freqFilePath = "E:/git-repository/git/bigdata-core/data/20120214/text_url/sorted/part-r-00000";
        freqFilePath = "E:/git-repository/git/bigdata-core/data/20120214/pic_url/sorted/part-r-00000";
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        final UserRecommender recommender = new UserRecommender(conf, mapFilePath, freqFilePath);
        final List<RecommendUserInfo> recommendedUserInfos = recommender.getRecommendedUser();
        for (final RecommendUserInfo userInfo : recommendedUserInfos) {
            System.out.println(userInfo);
        }
        System.out.println(recommendedUserInfos.size());
    }
}
