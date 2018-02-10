package com.dataliance.analysis.data.flow.impl;

import com.dataliance.analysis.data.flow.*;
import org.apache.log4j.*;
import com.dataliance.analysis.data.flow.model.*;
import com.dataliance.bdp.hbase.*;
import java.text.*;
import java.util.*;
import org.mortbay.log.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class FlowQueryImpl implements FlowQuery
{
    private static final Logger LOG;
    
    @Override
    public List<FlowModel> getFlowModelListByParam(final Map paramaterMap) {
        final List<String> region = paramaterMap.get("region");
        final String startDate = paramaterMap.get("startDate");
        final String endDate = paramaterMap.get("endDate");
        final List<FlowModel> list = new ArrayList<FlowModel>();
        final String uri = "/user/demo/usrPortrait/flow";
        final Configuration conf = HBaseHelper.getInstance().getConfiguration();
        try {
            final FileSystem fs = FileSystem.get(conf);
            final FileStatus[] arr$;
            final FileStatus[] datefiles = arr$ = fs.listStatus(new Path(uri), (PathFilter)new PathFilter() {
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
            for (final FileStatus datefile : arr$) {
                final Path logPath = datefile.getPath();
                FlowQueryImpl.LOG.info((Object)("\u67e5\u8be2\u6d41\u91cf\u7edf\u8ba1\u76ee\u5f55\uff1a" + logPath.getName()));
                final FileStatus[] arr$2;
                final FileStatus[] logfiles = arr$2 = logPath.getFileSystem(conf).listStatus(logPath, (PathFilter)new PathFilter() {
                    public boolean accept(final Path path) {
                        return path.getName().indexOf("part") >= 0;
                    }
                });
                for (final FileStatus logfile : arr$2) {
                    FlowQueryImpl.LOG.info((Object)("part\u6587\u4ef6\u540d:" + logfile.getPath().getName()));
                    final InputStream in = (InputStream)fs.open(logfile.getPath());
                    final BufferedReader lis = new BufferedReader(new InputStreamReader(in));
                    for (String str = lis.readLine(); str != null; str = lis.readLine()) {
                        final String[] arr = str.split("\\s+");
                        if (arr.length == 2) {
                            final String area = arr[0];
                            if (region == null || region.contains(area)) {
                                final String flow = arr[1];
                                final FlowModel model = new FlowModel();
                                model.setRegion(area);
                                model.setDate(logPath.getName());
                                long l = -1L;
                                try {
                                    l = new Long(flow);
                                }
                                catch (NumberFormatException e2) {
                                    Log.info("\u8f6c\u6362Long\u578b\u6d41\u91cf\u503c\u5f02\u5e38\uff1a" + model.toString());
                                }
                                model.setFlow(l);
                                FlowQueryImpl.LOG.info((Object)model.toString());
                                list.add(model);
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
        LOG = Logger.getLogger((Class)FlowQueryImpl.class);
    }
}
