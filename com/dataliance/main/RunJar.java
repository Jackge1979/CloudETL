package com.dataliance.main;

import java.util.zip.*;
import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;
import java.lang.reflect.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import com.dataliance.hadoop.*;

public class RunJar
{
    public static void unJar(final File jarFile, final File toDir) throws IOException {
        final JarFile jar = new JarFile(jarFile);
        try {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    final InputStream in = jar.getInputStream(entry);
                    try {
                        final File file = new File(toDir, entry.getName());
                        if (!file.getParentFile().mkdirs() && !file.getParentFile().isDirectory()) {
                            throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
                        }
                        final OutputStream out = new FileOutputStream(file);
                        try {
                            final byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        }
                        finally {
                            out.close();
                        }
                    }
                    finally {
                        in.close();
                    }
                }
            }
        }
        finally {
            jar.close();
        }
    }
    
    public static void addDir(final String s) throws IOException {
        try {
            final Field field = ClassLoader.class.getDeclaredField("usr_paths");
            field.setAccessible(true);
            final String[] paths = (String[])field.get(null);
            for (int i = 0; i < paths.length; ++i) {
                if (s.equals(paths[i])) {
                    return;
                }
            }
            final String[] tmp = new String[paths.length + 1];
            System.arraycopy(paths, 0, tmp, 0, paths.length);
            tmp[paths.length] = s;
            field.set(null, tmp);
        }
        catch (IllegalAccessException e) {
            throw new IOException("Failed to get permissions to set library path");
        }
        catch (NoSuchFieldException e2) {
            throw new IOException("Failed to get field handle to set library path");
        }
    }
    
    public static void runJar(final Configuration conf, final String jarName, final String runClass, final String[] args) throws Throwable {
        final int firstArg = 0;
        final File file = new File(jarName);
        String mainClassName = null;
        JarFile jarFile;
        try {
            jarFile = new JarFile(jarName);
        }
        catch (IOException io) {
            throw new IOException("Error opening job jar: " + jarName).initCause(io);
        }
        final Manifest manifest = jarFile.getManifest();
        if (manifest != null) {
            mainClassName = manifest.getMainAttributes().getValue("Main-Class");
        }
        jarFile.close();
        if (mainClassName == null) {
            mainClassName = runClass;
        }
        mainClassName = mainClassName.replaceAll("/", ".");
        final File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
        tmpDir.mkdirs();
        if (!tmpDir.isDirectory()) {
            System.err.println("Mkdirs failed to create " + tmpDir);
            System.exit(-1);
        }
        final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
        workDir.delete();
        workDir.mkdirs();
        if (!workDir.isDirectory()) {
            System.err.println("Mkdirs failed to create " + workDir);
            System.exit(-1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    FileUtil.fullyDelete(workDir);
                }
                catch (Exception ex) {}
            }
        });
        unJar(file, workDir);
        final ArrayList<URL> classPath = new ArrayList<URL>();
        classPath.add(new File(workDir + "/").toURI().toURL());
        classPath.add(file.toURI().toURL());
        classPath.add(new File(workDir, "classes/").toURI().toURL());
        final File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
            for (int i = 0; i < libs.length; ++i) {
                classPath.add(libs[i].toURI().toURL());
            }
        }
        initJavaLibrayPath(conf);
        final ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
        Thread.currentThread().setContextClassLoader(loader);
        final Class<?> mainClass = Class.forName(mainClassName, true, loader);
        final Method main = mainClass.getMethod("main", Array.newInstance(String.class, 0).getClass());
        final String[] newArgs = Arrays.asList(args).subList(firstArg, args.length).toArray(new String[0]);
        try {
            main.invoke(null, newArgs);
        }
        catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }
    
    public static Job runJar(final Configuration conf, final String jarName, final AbstractTool toolClass, final String[] args) throws Throwable {
        final File file = new File(jarName);
        final File tmpDir = new File(conf.get("hadoop.tmp.dir"));
        tmpDir.mkdirs();
        if (!tmpDir.isDirectory()) {
            System.err.println("Mkdirs failed to create " + tmpDir);
            System.exit(-1);
        }
        final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
        workDir.delete();
        workDir.mkdirs();
        if (!workDir.isDirectory()) {
            System.err.println("Mkdirs failed to create " + workDir);
            System.exit(-1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    FileUtil.fullyDelete(workDir);
                }
                catch (Exception ex) {}
            }
        });
        unJar(file, workDir);
        final ArrayList<URL> classPath = new ArrayList<URL>();
        classPath.add(new File(workDir + "/").toURI().toURL());
        classPath.add(file.toURI().toURL());
        classPath.add(new File(workDir, "classes/").toURI().toURL());
        final File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
            for (int i = 0; i < libs.length; ++i) {
                classPath.add(libs[i].toURI().toURL());
            }
        }
        final ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
        initJavaLibrayPath(conf);
        Thread.currentThread().setContextClassLoader(loader);
        run(conf, (Tool)toolClass, args);
        return toolClass.getJob();
    }
    
    public static int run(final Configuration conf, final Tool tool, final String[] args) throws Exception {
        if (tool.getConf() == null) {
            tool.setConf(conf);
        }
        final GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        final String[] toolArgs = parser.getRemainingArgs();
        return tool.run(toolArgs);
    }
    
    public static void initJavaLibrayPath(final Configuration conf) throws IOException {
        final String hadoopHome = conf.get("com.DA.hadoop.home", "/opt/hadoop/");
        final String platForm = PlatformName.getPlatformName();
        final String javaLibPath = hadoopHome + "/lib/native/" + platForm;
        addDir(javaLibPath);
    }
}
