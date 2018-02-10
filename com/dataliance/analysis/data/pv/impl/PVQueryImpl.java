package com.dataliance.analysis.data.pv.impl;

import org.apache.log4j.*;

import java.util.*;

import com.dataliance.analysis.data.pv.*;
import com.dataliance.analysis.data.pv.model.*;
import com.dataliance.bdp.hbase.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class PVQueryImpl implements PVQuery
{
    private static final Logger LOG;
    
    @Override
    public List<PVModel> getPVModelList(final Map<String, String> paramaterMap) {
        final String region = paramaterMap.get("region");
        final String date = paramaterMap.get("date");
        final List<PVModel> list = new ArrayList<PVModel>();
        final String uri = "/user/demo/usrPortrait/pv";
        final Configuration conf = HBaseHelper.getInstance().getConfiguration();
        try {
            final FileSystem fs = FileSystem.get(conf);
            final FileStatus[] arr$;
            final FileStatus[] datefiles = arr$ = fs.listStatus(new Path(uri), (PathFilter)new PathFilter() {
                public boolean accept(final Path path) {
                    return path.getName().equals(date);
                }
            });
            for (final FileStatus datefile : arr$) {
                final Path logPath = datefile.getPath();
                PVQueryImpl.LOG.info((Object)("\u67e5\u8be2PV\u76ee\u5f55\uff1a" + logPath.getName()));
                final FileStatus[] arr$2;
                final FileStatus[] logfiles = arr$2 = logPath.getFileSystem(conf).listStatus(logPath, (PathFilter)new PathFilter() {
                    public boolean accept(final Path path) {
                        return path.getName().indexOf("part") >= 0;
                    }
                });
                for (final FileStatus logfile : arr$2) {
                    PVQueryImpl.LOG.info((Object)("part\u6587\u4ef6\u540d:" + logfile.getPath().getName()));
                    final InputStream in = (InputStream)fs.open(logfile.getPath());
                    final BufferedReader lis = new BufferedReader(new InputStreamReader(in));
                    for (String str = lis.readLine(); str != null; str = lis.readLine()) {
                        final String[] arr = str.split("\\s+");
                        if (arr.length == 3) {
                            final String area = arr[0];
                            if (region == null || region.equals(area)) {
                                final int hour = new Integer(arr[1]);
                                int num;
                                try {
                                    num = Integer.valueOf(arr[2]);
                                }
                                catch (NumberFormatException e2) {
                                    num = 0;
                                }
                                final PVModel pvModel = new PVModel();
                                pvModel.setRegion(area);
                                pvModel.setHour(hour);
                                pvModel.setNum(num);
                                list.add(pvModel);
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
        LOG = Logger.getLogger((Class)PVQueryImpl.class);
    }
}
