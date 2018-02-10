package com.dataliance.util;

import java.util.*;
import org.apache.tools.tar.*;
import java.util.zip.*;
import java.io.*;

public class TarUtils
{
    private static int BUFFER;
    private static byte[] B_ARRAY;
    
    public void execute(final String inputFileName, final String targetFileName) {
        final File inputFile = new File(inputFileName);
        final String base = inputFileName.substring(inputFileName.lastIndexOf("/") + 1);
        final TarOutputStream out = this.getTarOutputStream(targetFileName);
        this.tarPack(out, inputFile, base);
        try {
            if (null != out) {
                out.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        this.compress(new File(targetFileName));
    }
    
    public void execute(final List<String> inputFileNameList, final String targetFileName) {
        final TarOutputStream out = this.getTarOutputStream(targetFileName);
        for (final String inputFileName : inputFileNameList) {
            final File inputFile = new File(inputFileName);
            final String base = inputFileName.substring(inputFileName.lastIndexOf("/") + 1);
            this.tarPack(out, inputFile, base);
        }
        try {
            if (null != out) {
                out.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void tarPack(final TarOutputStream out, final File inputFile, final String base) {
        if (inputFile.isDirectory()) {
            this.packFolder(out, inputFile, base);
        }
        else {
            this.packFile(out, inputFile, base);
        }
    }
    
    private void packFolder(final TarOutputStream out, final File inputFile, String base) {
        final File[] fileList = inputFile.listFiles();
        try {
            out.putNextEntry(new TarEntry(base + "/"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        base = ((base.length() == 0) ? "" : (base + "/"));
        for (final File file : fileList) {
            this.tarPack(out, file, base + file.getName());
        }
    }
    
    private void packFile(final TarOutputStream out, final File inputFile, final String base) {
        final TarEntry tarEntry = new TarEntry(base);
        System.out.println(inputFile + " len = " + inputFile.length());
        tarEntry.setSize(inputFile.length());
        try {
            out.putNextEntry(tarEntry);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        FileInputStream in = null;
        try {
            in = new FileInputStream(inputFile);
        }
        catch (FileNotFoundException e2) {
            e2.printStackTrace();
        }
        int b = 0;
        try {
            while ((b = in.read(TarUtils.B_ARRAY, 0, TarUtils.BUFFER)) != -1) {
                out.write(TarUtils.B_ARRAY, 0, b);
            }
        }
        catch (IOException e3) {
            e3.printStackTrace();
        }
        catch (NullPointerException e4) {
            System.err.println("NullPointerException info ======= [FileInputStream is null]");
        }
        finally {
            try {
                if (null != in) {
                    in.close();
                }
                if (null != out) {
                    out.closeEntry();
                }
            }
            catch (IOException ex) {}
        }
    }
    
    private void compress(final File srcFile) {
        final File target = new File(srcFile.getAbsolutePath() + ".gz");
        FileInputStream in = null;
        GZIPOutputStream out = null;
        try {
            in = new FileInputStream(srcFile);
            out = new GZIPOutputStream(new FileOutputStream(target));
            int number = 0;
            while ((number = in.read(TarUtils.B_ARRAY, 0, TarUtils.BUFFER)) != -1) {
                out.write(TarUtils.B_ARRAY, 0, number);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e2) {
            e2.printStackTrace();
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            }
            catch (IOException e3) {
                e3.printStackTrace();
            }
        }
    }
    
    private TarOutputStream getTarOutputStream(String targetFileName) {
        targetFileName = (targetFileName.endsWith(".tar") ? targetFileName : (targetFileName + ".tar"));
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(targetFileName);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        final TarOutputStream out = new TarOutputStream((OutputStream)bufferedOutputStream);
        out.setLongFileMode(2);
        return out;
    }
    
    static {
        TarUtils.BUFFER = 4096;
        TarUtils.B_ARRAY = new byte[TarUtils.BUFFER];
    }
}
