package com.dataliance.hadoop.mapreduce.input;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.logging.*;

public class FileKeyLineRecordReader extends RecordReader<FileKey, Text>
{
    private static final Log LOG;
    private CompressionCodecFactory compressionCodecs;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private FileKey key;
    private Text value;
    private String path;
    
    public FileKeyLineRecordReader() {
        this.compressionCodecs = null;
        this.key = new FileKey();
        this.value = null;
    }
    
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit)genericSplit;
        final Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(job);
        if (this.key == null) {
            this.key = new FileKey();
        }
        this.path = file.toString();
        context.setStatus("init path = " + this.path);
        this.key.setPath(this.path);
        this.compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = this.compressionCodecs.getCodec(file);
        final FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            this.in = new LineReader((InputStream)codec.createInputStream((InputStream)fileIn), job);
            this.end = Long.MAX_VALUE;
        }
        else {
            if (this.start != 0L) {
                skipFirstLine = true;
                fileIn.seek(--this.start);
            }
            this.in = new LineReader((InputStream)fileIn, job);
        }
        if (skipFirstLine) {
            this.start += this.in.readLine(new Text(), 0, (int)Math.min(2147483647L, this.end - this.start));
        }
        this.pos = this.start;
    }
    
    public boolean nextKeyValue() throws IOException {
        if (this.key == null) {
            this.key = new FileKey();
        }
        this.key.setPath(this.path);
        this.key.setPos(this.pos);
        if (this.value == null) {
            this.value = new Text();
        }
        int newSize = 0;
        while (this.pos < this.end) {
            newSize = this.in.readLine(this.value, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));
            if (newSize == 0) {
                break;
            }
            this.pos += newSize;
            if (newSize < this.maxLineLength) {
                break;
            }
            FileKeyLineRecordReader.LOG.info((Object)("Skipped line of size " + newSize + " at pos " + (this.pos - newSize)));
        }
        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        }
        return true;
    }
    
    public FileKey getCurrentKey() {
        return this.key;
    }
    
    public Text getCurrentValue() {
        return this.value;
    }
    
    public float getProgress() {
        if (this.start == this.end) {
            return 0.0f;
        }
        return Math.min(1.0f, (this.pos - this.start) / (this.end - this.start));
    }
    
    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
    
    static {
        LOG = LogFactory.getLog((Class)FileKeyLineRecordReader.class);
    }
}
