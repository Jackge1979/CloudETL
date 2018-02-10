package com.dataliance.analysis.data.terminal.impl;

import com.dataliance.analysis.data.terminal.*;
import org.apache.log4j.*;
import com.dataliance.analysis.data.terminal.model.*;
import java.util.*;
import com.dataliance.bdp.hbase.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class TerminalCountSortQueryImpl implements TerminalCountSortQuery
{
    private static final Logger LOG;
    
    @Override
    public List<TerminalCountModel> getTerminalCountSort(final Map<String, String> paramaterMap) {
        final String date = paramaterMap.get("date");
        final List<TerminalCountModel> list = new ArrayList<TerminalCountModel>();
        final String uri = "/user/demo/usrPortrait/terminalSort";
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
                Path logPath = datefile.getPath();
                TerminalCountSortQueryImpl.LOG.info((Object)("\u67e5\u8be2\u7ec8\u7aef\u6392\u5e8f\u76ee\u5f55\uff1a" + logPath.getName()));
                logPath = new Path(logPath, "sort");
                final FileStatus[] arr$2;
                final FileStatus[] logfiles = arr$2 = logPath.getFileSystem(conf).listStatus(logPath, (PathFilter)new PathFilter() {
                    public boolean accept(final Path path) {
                        return path.getName().indexOf("part") >= 0;
                    }
                });
                for (final FileStatus logfile : arr$2) {
                    TerminalCountSortQueryImpl.LOG.info((Object)("part\u6587\u4ef6\u540d:" + logfile.getPath().getName()));
                    final InputStream in = (InputStream)fs.open(logfile.getPath());
                    final BufferedReader lis = new BufferedReader(new InputStreamReader(in));
                    for (String str = lis.readLine(); str != null && list.size() <= 21; str = lis.readLine()) {
                        final String[] arr = str.split("@@@");
                        if (arr.length == 2) {
                            final int sum = new Integer(arr[0]);
                            final String terminal = arr[1];
                            final TerminalCountModel tm = new TerminalCountModel();
                            tm.setNum(sum);
                            tm.setTerminal(terminal);
                            list.add(tm);
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
        LOG = Logger.getLogger((Class)TerminalCountSortQueryImpl.class);
    }
}
