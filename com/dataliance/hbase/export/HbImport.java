package com.dataliance.hbase.export;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.*;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.fs.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;

import com.dataliance.hbase.vo.*;

public class HbImport extends Configured
{
    private HBaseAdmin hbaseAdmin;
    private FileSystem fs;
    
    public HbImport(final Configuration conf) throws IOException {
        super(conf);
        this.fs = FileSystem.get(conf);
        this.hbaseAdmin = new HBaseAdmin(conf);
    }
    
    public void doImport(final Path path) throws Exception {
        final FileStatus[] arr$;
        final FileStatus[] fStatus = arr$ = this.fs.listStatus(path, (PathFilter)new PathFilter() {
            public boolean accept(final Path path) {
                return path.getName().endsWith(".file");
            }
        });
        for (final FileStatus fStatu : arr$) {
            final Path inFile = fStatu.getPath();
            final SequenceFile.Reader reader = new SequenceFile.Reader(this.fs, inFile, this.getConf());
            final Text key = new Text();
            final HbExportVO hbe = new HbExportVO();
            for (boolean hasNext = reader.next((Writable)key); hasNext; hasNext = reader.next((Writable)key)) {
                reader.getCurrentValue((Writable)hbe);
                final HTableDescriptor td = new HTableDescriptor(Bytes.toBytes(hbe.getTableName()));
                for (final String family : hbe) {
                    td.addFamily(new HColumnDescriptor(family));
                }
                this.hbaseAdmin.createTable(td);
                final Path dataPath = new Path(path, hbe.getTableName());
                if (this.fs.exists(dataPath)) {
                    final Job job = Import.createSubmittableJob(this.getConf(), new String[] { hbe.getTableName(), dataPath.toString() });
                    job.waitForCompletion(true);
                }
            }
            reader.close();
        }
    }
}
