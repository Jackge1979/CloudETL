package com.dataliance.analysis.data.region.impl;

import com.dataliance.analysis.data.region.*;
import org.apache.log4j.*;
import com.dataliance.analysis.data.region.model.*;
import com.dataliance.bdp.hbase.*;
import java.text.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class RegionDistributionQueryImpl implements RegionDistributionQuery
{
    private static final Logger LOG;
    
    @Override
    public List<RegionDistributionModel> getRegionDistributionByParamater(final Map paramaterMap) {
        final List<String> region = paramaterMap.get("region");
        final String startDate = paramaterMap.get("startDate");
        final String endDate = paramaterMap.get("endDate");
        final List<RegionDistributionModel> list = new ArrayList<RegionDistributionModel>();
        final String uri = "/user/demo/usrPortrait/regionDistribution";
        final Configuration conf = HBaseHelper.getInstance().getConfiguration();
        try {
            final FileSystem fs = FileSystem.get(conf);
            final FileStatus[] datefiles = fs.listStatus(new Path(uri), (PathFilter)new PathFilter() {
                public boolean accept(final Path path) {
                    final String p = path.getName();
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd", Locale.getDefault());
                    try {
                        final Date start = sdf.parse(startDate);
                        final Date end = sdf.parse(endDate);
                        final Date now = sdf.parse(p);
                        if (!start.after(now) && !now.after(end)) {
                            return true;
                        }
                    }
                    catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return false;
                }
            });
            System.out.println("datafiles size:" + datefiles.length);
            for (final FileStatus datefile : datefiles) {
                Path logPath = datefile.getPath();
                final String date = logPath.getName();
                logPath = new Path(logPath, "count");
                System.out.println("logPath Name:" + logPath.getName());
                final FileStatus[] arr$2;
                final FileStatus[] logfiles = arr$2 = logPath.getFileSystem(conf).listStatus(logPath, (PathFilter)new PathFilter() {
                    public boolean accept(final Path path) {
                        RegionDistributionQueryImpl.LOG.warn((Object)("log Name1:" + path.getName()));
                        return path.getName().indexOf("part") >= 0;
                    }
                });
                for (final FileStatus logfile : arr$2) {
                    RegionDistributionQueryImpl.LOG.warn((Object)("log Name2:" + logfile.getPath().getName()));
                    final InputStream in = (InputStream)fs.open(logfile.getPath());
                    final BufferedReader lis = new BufferedReader(new InputStreamReader(in));
                    for (String str = lis.readLine(); str != null; str = lis.readLine()) {
                        final String[] arr = str.split("\\s+");
                        if (arr.length == 2) {
                            final String area = arr[0];
                            if (region == null || region.contains(area)) {
                                final int sum = new Integer(arr[1]);
                                final RegionDistributionModel rd = new RegionDistributionModel();
                                rd.setRegion(area);
                                rd.setDate(date);
                                rd.setNum(sum);
                                list.add(rd);
                            }
                        }
                    }
                    in.close();
                    lis.close();
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
    
    static {
        LOG = Logger.getLogger((Class)RegionDistributionQueryImpl.class);
    }
}
