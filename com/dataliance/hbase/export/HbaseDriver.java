package com.dataliance.hbase.export;

import org.apache.hadoop.fs.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;

public class HbaseDriver
{
    public static final String FILE_SUFFIX = ".file";
    
    public static void main(final String[] args) throws Exception {
        final String useAge = "<-import|-export [[-tn <tableName>] [-num <num>]]> <path>";
        if (args.length < 2) {
            System.err.println(useAge);
            return;
        }
        final String opt = args[0];
        final Configuration conf = DAConfigUtil.create();
        if (opt.equals("-import")) {
            final String path = args[1];
            final HbImport hbImport = new HbImport(conf);
            hbImport.doImport(new Path(path));
        }
        else {
            if (!opt.equals("-export")) {
                System.err.println(useAge);
                return;
            }
            final String exOpt = args[1];
            final HbExport hbExport = new HbExport(conf);
            if (exOpt.equals("-tn")) {
                final String tableName = args[2];
                final String option = args[3];
                if (option.equals("-num")) {
                    final long num = Long.parseLong(args[4]);
                    final String file = args[5];
                    hbExport.doExport(new Path(file), tableName, num);
                }
                else {
                    final String file2 = args[3];
                    hbExport.doExport(new Path(file2), tableName, -1L);
                }
            }
            else if (exOpt.equals("-num")) {
                final long num2 = Long.parseLong(args[2]);
                final String file2 = args[3];
                hbExport.doExport(new Path(file2), num2);
            }
            else {
                hbExport.doExport(new Path(exOpt), -1L);
            }
        }
    }
}
