package com.dataliance.main;

import java.util.*;
import org.apache.hadoop.util.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;

public class Main
{
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage : Main <class> args...");
            return;
        }
        final Configuration conf = DAConfigUtil.create();
        int firstArg = 0;
        final String clazz = args[firstArg++];
        final Class<?> cl = Class.forName(clazz, true, Main.class.getClassLoader());
        final Tool tool = (Tool)cl.newInstance();
        final String[] newArgs = Arrays.asList(args).subList(firstArg, args.length).toArray(new String[0]);
        ToolRunner.run(conf, tool, newArgs);
    }
}
