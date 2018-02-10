package com.dataliance.hadoop.util;

import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import java.util.*;

public class HadoopFSUtil
{
    public static final PathFilter hiddenFileFilter;
    
    public static PathFilter getPassAllFilter() {
        return (PathFilter)new PathFilter() {
            public boolean accept(final Path arg0) {
                return true;
            }
        };
    }
    
    public static PathFilter getPassDirectoriesFilter(final FileSystem fs) {
        return (PathFilter)new PathFilter() {
            public boolean accept(final Path path) {
                try {
                    return fs.getFileStatus(path).isDir();
                }
                catch (IOException ioe) {
                    return false;
                }
            }
        };
    }
    
    public static Path[] getPaths(final FileStatus[] stats) {
        if (stats == null) {
            return null;
        }
        if (stats.length == 0) {
            return new Path[0];
        }
        final Path[] res = new Path[stats.length];
        for (int i = 0; i < stats.length; ++i) {
            res[i] = stats[i].getPath();
        }
        return res;
    }
    
    public static FileStatus[] listPath(final FileSystem fs, final Path dir) throws IOException {
        return fs.listStatus(dir, HadoopFSUtil.hiddenFileFilter);
    }
    
    public static MapFile.Reader[] getReaders(final FileSystem fs, final Path dir, final Configuration conf) throws IOException {
        final Path[] names = FileUtil.stat2Paths(fs.listStatus(dir, HadoopFSUtil.hiddenFileFilter));
        Arrays.sort(names);
        final MapFile.Reader[] parts = new MapFile.Reader[names.length];
        for (int i = 0; i < names.length; ++i) {
            parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
        }
        return parts;
    }
    
    static {
        hiddenFileFilter = (PathFilter)new PathFilter() {
            public boolean accept(final Path p) {
                final String name = p.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            }
        };
    }
}
