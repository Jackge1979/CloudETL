package com.dataliance.util;

import java.util.*;
import org.apache.hadoop.util.*;
import java.io.*;

public class DomainSuffixes
{
    private HashMap<String, DomainSuffix> domains;
    private static DomainSuffixes instance;
    
    private DomainSuffixes() {
        this.domains = new HashMap<String, DomainSuffix>();
        final String file = "domain-suffixes.xml";
        final InputStream input = this.getClass().getClassLoader().getResourceAsStream(file);
        try {
            new DomainSuffixesReader().read(this, input);
        }
        catch (Exception ex) {
            StringUtils.stringifyException((Throwable)ex);
        }
    }
    
    public static DomainSuffixes getInstance() {
        if (DomainSuffixes.instance == null) {
            DomainSuffixes.instance = new DomainSuffixes();
        }
        return DomainSuffixes.instance;
    }
    
    void addDomainSuffix(final DomainSuffix tld) {
        this.domains.put(tld.getDomain(), tld);
    }
    
    public boolean isDomainSuffix(final String extension) {
        return this.domains.containsKey(extension);
    }
    
    public DomainSuffix get(final String extension) {
        return this.domains.get(extension);
    }
}
