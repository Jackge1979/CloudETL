package com.dataliance.service.util;

import java.text.*;
import java.util.*;

public class ThreadTracer
{
    public static void printAllThread() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        for (ThreadGroup parent = null; (parent = group.getParent()) != null; group = parent) {}
        final Thread[] threads = new Thread[group.activeCount()];
        group.enumerate(threads);
        final HashSet<String> set = new HashSet<String>();
        for (int i = 0; i < threads.length; ++i) {
            if (threads[i] != null && threads[i].isAlive()) {
                try {
                    set.add(threads[i].getThreadGroup().getName() + "," + threads[i].getName() + "," + threads[i].getPriority() + "," + threads[i].isDaemon());
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
        final String[] result = set.toArray(new String[0]);
        Arrays.sort(result);
        final StringBuffer buffer = new StringBuffer();
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        buffer.append(String.format("\n=============== %s ===============\n", format.format(new Date())));
        for (final String name : result) {
            buffer.append(name);
            buffer.append("\n");
        }
        System.out.println(buffer.toString());
    }
}
