package com.dataliance.hbase.export;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;

import com.dataliance.hbase.mapreduce.*;
import com.dataliance.hbase.vo.*;

public class HbExport extends Configured
{
    private HBaseAdmin hbaseAdmin;
    private FileSystem fs;
    
    public HbExport(final Configuration conf) throws IOException {
        super(conf);
        this.fs = FileSystem.get(conf);
        this.hbaseAdmin = new HBaseAdmin(conf);
    }
    
    public void doExport(final Path outPath, final long num) throws Exception {
        this.fs.mkdirs(outPath);
        final HTableDescriptor[] arr$;
        final HTableDescriptor[] hds = arr$ = this.hbaseAdmin.listTables();
        for (final HTableDescriptor hd : arr$) {
            final HbExportVO heo = new HbExportVO();
            heo.setTableName(Bytes.toString(hd.getName()));
            final Collection<HColumnDescriptor> hchs = (Collection<HColumnDescriptor>)hd.getFamilies();
            for (final HColumnDescriptor hcd : hchs) {
                heo.addFamily(Bytes.toString(hcd.getName()));
            }
            final Path outFile = new Path(outPath, heo.getTableName() + ".file");
            final SequenceFile.Writer sw = SequenceFile.createWriter(this.fs, this.getConf(), outFile, (Class)Text.class, (Class)HbExportVO.class);
            sw.append((Writable)new Text(heo.getTableName()), (Writable)heo);
            sw.close();
            if (num != 0L) {
                final Job job = Export.createSubmittableJob(this.getConf(), new String[] { heo.getTableName(), new Path(outPath, heo.getTableName()).toString() });
                if (num > 0L) {
                    job.setMapperClass((Class)DAExporter.class);
                    job.getConfiguration().setLong("DA.hbase.export.rownum", num);
                }
                job.waitForCompletion(true);
            }
        }
    }
    
    public void doExport(final Path outPath, final String tableName, final long num) throws Exception {
        this.fs.mkdirs(outPath);
        final HTableDescriptor hd = this.hbaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
        final HbExportVO heo = new HbExportVO();
        heo.setTableName(Bytes.toString(hd.getName()));
        final Collection<HColumnDescriptor> hchs = (Collection<HColumnDescriptor>)hd.getFamilies();
        for (final HColumnDescriptor hcd : hchs) {
            heo.addFamily(Bytes.toString(hcd.getName()));
        }
        final Path outFile = new Path(outPath, tableName + ".file");
        final SequenceFile.Writer sw = SequenceFile.createWriter(this.fs, this.getConf(), outFile, (Class)Text.class, (Class)HbExportVO.class);
        sw.append((Writable)new Text(heo.getTableName()), (Writable)heo);
        sw.close();
        final Job job = Export.createSubmittableJob(this.getConf(), new String[] { tableName, new Path(outPath, tableName).toString() });
        if (num > 0L) {
            job.setMapperClass((Class)DAExporter.class);
            job.getConfiguration().setLong("DA.hbase.export.rownum", num);
        }
        job.waitForCompletion(true);
    }
}
