package com.dataliance.analysis.data.recommend;

import java.io.*;
import java.util.*;

public class AccessedFrequencyManager
{
    private List<AccessedFrequency> accessedFrequencys;
    
    public AccessedFrequencyManager(final String filePath) {
        this.accessedFrequencys = new ArrayList<AccessedFrequency>();
        this.loadSatisticResult(filePath);
    }
    
    private void loadSatisticResult(final String filePath) {
        try {
            final BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
            String line = null;
            String[] values = null;
            while ((line = reader.readLine()) != null) {
                values = line.split("\t");
                if (values.length != 2) {
                    continue;
                }
                this.accessedFrequencys.add(new AccessedFrequency(Long.parseLong(values[0]), values[1]));
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e2) {
            e2.printStackTrace();
        }
    }
    
    public List<AccessedFrequency> getAccessedFrequencys() {
        return this.accessedFrequencys;
    }
    
    public void printForDebug() {
        for (final AccessedFrequency fre : this.accessedFrequencys) {
            System.out.println(fre);
        }
    }
    
    public static void main(final String[] args) {
        final String filePath = "E:/git-repository/git/bigdata-core/data/20120214/text_url/sorted/part-r-00000";
        final AccessedFrequencyManager join = new AccessedFrequencyManager(filePath);
        join.printForDebug();
    }
}
