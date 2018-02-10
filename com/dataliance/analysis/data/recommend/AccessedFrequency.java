package com.dataliance.analysis.data.recommend;

import java.util.*;

public class AccessedFrequency
{
    private String phoneNumber;
    private long freq;
    
    public AccessedFrequency() {
        this.phoneNumber = null;
        this.freq = 0L;
    }
    
    public AccessedFrequency(final long freq, final String phoneNumber) {
        this.phoneNumber = null;
        this.freq = 0L;
        this.freq = freq;
        this.phoneNumber = phoneNumber;
    }
    
    public String getPhoneNumber() {
        return this.phoneNumber;
    }
    
    public void setPhoneNumber(final String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
    
    public long getFreq() {
        return this.freq;
    }
    
    public void setFreq(final long freq) {
        this.freq = freq;
    }
    
    @Override
    public String toString() {
        return String.format("%s , %s", this.freq, this.phoneNumber);
    }
    
    public static class FrequencyComparator implements Comparator
    {
        @Override
        public int compare(final Object o1, final Object o2) {
            final AccessedFrequency phoneFreq1 = (AccessedFrequency)o1;
            final AccessedFrequency phoneFreq2 = (AccessedFrequency)o2;
            if (phoneFreq1.getFreq() == phoneFreq2.getFreq()) {
                return 0;
            }
            if (phoneFreq1.getFreq() > phoneFreq2.getFreq()) {
                return -1;
            }
            return 1;
        }
    }
}
