package com.dataliance.analysis.data.uv.impl;

import com.dataliance.analysis.data.uv.*;
import com.dataliance.analysis.data.uv.model.*;
import java.util.*;
import com.dataliance.bdp.hbase.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;

public class UVQueryImpl implements UVQuery
{
    @Override
    public List<UVModel> getUVModelList(final Map<String, String> paramaterMap) {
        final String region = paramaterMap.get("region");
        final String date = paramaterMap.get("date");
        final List<UVModel> list = new ArrayList<UVModel>();
        final String uri = "/user/demo/usrPortrait/uv";
        final Configuration conf = HBaseHelper.getInstance().getConfiguration();
        try {
            final FileSystem fs = FileSystem.get(conf);
            final FileStatus[] datefiles = fs.listStatus(new Path(uri), (PathFilter)new PathFilter() {
                public boolean accept(final Path path) {
                    return path.getName().equals(date);
                }
            });
            System.out.println("datafiles size:" + datefiles.length);
            for (final FileStatus datefile : datefiles) {
                Path logPath = datefile.getPath();
                logPath = new Path(logPath, "count");
                System.out.println("logPath Name:" + logPath.getName());
                final FileStatus[] arr$2;
                final FileStatus[] logfiles = arr$2 = logPath.getFileSystem(conf).listStatus(logPath, (PathFilter)new PathFilter() {
                    public boolean accept(final Path path) {
                        System.out.println("log Name1:" + path.getName());
                        return path.getName().indexOf("part") >= 0;
                    }
                });
                for (final FileStatus logfile : arr$2) {
                    System.out.println("log Name2:" + logfile.getPath().getName());
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
                                final UVModel uvModel = new UVModel();
                                uvModel.setRegion(area);
                                uvModel.setHour(hour);
                                uvModel.setNum(num);
                                list.add(uvModel);
                                System.out.println(list.size());
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
}
