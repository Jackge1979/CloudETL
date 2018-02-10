package com.dataliance.util;

import java.util.*;
import org.apache.tools.zip.*;
import java.io.*;
import org.apache.hadoop.fs.*;

public class ZipUtil
{
    private static final int BUFFEREDSIZE = 1024;
    
    public synchronized void unzip(final String zipFileName, final String extPlace) throws Exception {
        try {
            new File(extPlace).mkdirs();
            final File f = new File(zipFileName);
            final ZipFile zipFile = new ZipFile(zipFileName);
            if (!f.exists() && f.length() <= 0L) {
                throw new Exception("\u8981\u89e3\u538b\u7684\u6587\u4ef6\u4e0d\u5b58\u5728!");
            }
            final File tempFile = new File(extPlace);
            final String strPath = tempFile.getAbsolutePath();
            final Enumeration e = zipFile.getEntries();
            while (e.hasMoreElements()) {
                final ZipEntry zipEnt = e.nextElement();
                String gbkPath = zipEnt.getName();
                if (zipEnt.isDirectory()) {
                    final String strtemp = strPath + File.separator + gbkPath;
                    final File dir = new File(strtemp);
                    dir.mkdirs();
                }
                else {
                    final InputStream is = zipFile.getInputStream(zipEnt);
                    final BufferedInputStream bis = new BufferedInputStream(is);
                    gbkPath = zipEnt.getName();
                    final String strtemp = strPath + File.separator + gbkPath;
                    final String strsubdir = gbkPath;
                    for (int i = 0; i < strsubdir.length(); ++i) {
                        if (strsubdir.substring(i, i + 1).equalsIgnoreCase("/")) {
                            final String temp = strPath + File.separator + strsubdir.substring(0, i);
                            final File subdir = new File(temp);
                            if (!subdir.exists()) {
                                subdir.mkdir();
                            }
                        }
                    }
                    final FileOutputStream fos = new FileOutputStream(strtemp);
                    final BufferedOutputStream bos = new BufferedOutputStream(fos);
                    int c;
                    while ((c = bis.read()) != -1) {
                        bos.write((byte)c);
                    }
                    bos.close();
                    fos.close();
                }
            }
        }
        catch (Exception e2) {
            e2.printStackTrace();
            throw e2;
        }
    }
    
    public synchronized void unzip(final String zipFileName, final String extPlace, final boolean whether) throws Exception {
        try {
            new File(extPlace).mkdirs();
            final File f = new File(zipFileName);
            final ZipFile zipFile = new ZipFile(zipFileName);
            if (!f.exists() && f.length() <= 0L) {
                throw new Exception("\u8981\u89e3\u538b\u7684\u6587\u4ef6\u4e0d\u5b58\u5728!");
            }
            final File tempFile = new File(extPlace);
            final String strPath = tempFile.getAbsolutePath();
            final Enumeration e = zipFile.getEntries();
            while (e.hasMoreElements()) {
                final ZipEntry zipEnt = e.nextElement();
                String gbkPath = zipEnt.getName();
                if (zipEnt.isDirectory()) {
                    final String strtemp = strPath + File.separator + gbkPath;
                    final File dir = new File(strtemp);
                    dir.mkdirs();
                }
                else {
                    final InputStream is = zipFile.getInputStream(zipEnt);
                    final BufferedInputStream bis = new BufferedInputStream(is);
                    gbkPath = zipEnt.getName();
                    final String strtemp = strPath + File.separator + gbkPath;
                    final String strsubdir = gbkPath;
                    for (int i = 0; i < strsubdir.length(); ++i) {
                        if (strsubdir.substring(i, i + 1).equalsIgnoreCase("/")) {
                            final String temp = strPath + File.separator + strsubdir.substring(0, i);
                            final File subdir = new File(temp);
                            if (!subdir.exists()) {
                                subdir.mkdir();
                            }
                        }
                    }
                    final FileOutputStream fos = new FileOutputStream(strtemp);
                    final BufferedOutputStream bos = new BufferedOutputStream(fos);
                    int c;
                    while ((c = bis.read()) != -1) {
                        bos.write((byte)c);
                    }
                    bos.close();
                    fos.close();
                }
            }
        }
        catch (Exception e2) {
            e2.printStackTrace();
            throw e2;
        }
    }
    
    public synchronized void zip(final String inputFilename, final String zipFilename) throws IOException {
        this.zip(new File(inputFilename), zipFilename);
    }
    
    public synchronized void zip(final File inputFile, final String zipFilename) throws IOException {
        final ZipOutputStream out = new ZipOutputStream((OutputStream)new FileOutputStream(zipFilename));
        try {
            this.zip(inputFile, out, "");
        }
        catch (IOException e) {
            throw e;
        }
        finally {
            out.close();
        }
    }
    
    private synchronized void zip(final File inputFile, final ZipOutputStream out, String base) throws IOException {
        if (inputFile.isDirectory()) {
            final File[] inputFiles = inputFile.listFiles();
            out.putNextEntry(new ZipEntry(base + "/"));
            base = ((base.length() == 0) ? "" : (base + "/"));
            for (int i = 0; i < inputFiles.length; ++i) {
                this.zip(inputFiles[i], out, base + inputFiles[i].getName());
            }
        }
        else {
            if (base.length() > 0) {
                out.putNextEntry(new ZipEntry(base));
            }
            else {
                out.putNextEntry(new ZipEntry(inputFile.getName()));
            }
            final FileInputStream in = new FileInputStream(inputFile);
            try {
                final byte[] by = new byte[1024];
                int c;
                while ((c = in.read(by)) != -1) {
                    out.write(by, 0, c);
                }
            }
            catch (IOException e) {
                throw e;
            }
            finally {
                in.close();
            }
        }
    }
    
    public synchronized void zip(final FileSystem fs, final Path inputFile, final String zipFilename) throws IOException {
        final ZipOutputStream out = new ZipOutputStream((OutputStream)new FileOutputStream(zipFilename));
        try {
            this.zip(fs, inputFile, out, "");
        }
        finally {
            out.close();
        }
    }
    
    private void zip(final FileSystem fs, final Path src, final ZipOutputStream out, String base) throws IOException {
        final FileStatus fStatus = fs.getFileStatus(src);
        if (fStatus.isDir()) {
            final FileStatus[] fStatuses = fs.listStatus(src);
            out.putNextEntry(new ZipEntry(base + "/"));
            out.closeEntry();
            base = ((base.length() == 0) ? "/" : (base + "/"));
            for (final FileStatus fileSt : fStatuses) {
                this.zip(fs, fileSt.getPath(), out, base + fileSt.getPath().getName());
            }
        }
        else {
            if (base.length() > 0) {
                out.putNextEntry(new ZipEntry(base));
            }
            else {
                out.putNextEntry(new ZipEntry(src.getName()));
            }
            final InputStream in = (InputStream)fs.open(src);
            StreamUtil.output(in, (OutputStream)out, false);
            out.closeEntry();
        }
    }
}
