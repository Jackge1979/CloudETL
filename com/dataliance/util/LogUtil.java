package com.dataliance.util;

import java.lang.reflect.*;
import org.apache.commons.logging.*;
import java.io.*;
import org.slf4j.*;
import java.util.*;
import java.util.logging.Logger;

public class LogUtil
{
    private static final Log LOG;
    private static final Map<Class<?>, Log> LOG_CACHE;
    private static Method LOG_TRACE;
    private static Method LOG_DEBUG;
    private static Method LOG_INFO;
    private static Method LOG_WARN;
    private static Method LOG_ERROR;
    private static Method LOG_FATAL;
    private static Method LOGGER_TRACE;
    private static Method LOGGER_DEBUG;
    private static Method LOGGER_INFO;
    private static Method LOGGER_WARN;
    private static Method LOGGER_ERROR;
    
    public static Log getLog(final Class<?> clazz) {
        Log log = LogUtil.LOG_CACHE.get(clazz);
        if (log == null) {
            log = LogFactory.getLog((Class)clazz);
            LogUtil.LOG_CACHE.put(clazz, log);
        }
        return log;
    }
    
    public static void info(final Class<?> clazz, final Object msg) {
        getLog(clazz).info(msg);
    }
    
    public static void debug(final Class<?> clazz, final Object msg) {
        getLog(clazz).debug(msg);
    }
    
    public static void error(final Class<?> clazz, final Object msg) {
        getLog(clazz).error(msg);
    }
    
    public static PrintStream getTraceStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_TRACE);
    }
    
    public static PrintStream getDebugStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_DEBUG);
    }
    
    public static PrintStream getInfoStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_INFO);
    }
    
    public static PrintStream getWarnStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_WARN);
    }
    
    public static PrintStream getErrorStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_ERROR);
    }
    
    public static PrintStream getFatalStream(final Log logger) {
        return getLogStream(logger, LogUtil.LOG_FATAL);
    }
    
    private static PrintStream getLogStream(final Log logger, final Method method) {
        return new PrintStream(new ByteArrayOutputStream() {
            private int scan = 0;
            
            private boolean hasNewline() {
                while (this.scan < this.count) {
                    if (this.buf[this.scan] == 10) {
                        return true;
                    }
                    ++this.scan;
                }
                return false;
            }
            
            @Override
            public void flush() throws IOException {
                if (!this.hasNewline()) {
                    return;
                }
                try {
                    method.invoke(logger, this.toString().trim());
                }
                catch (Exception e) {
                    if (LogUtil.LOG.isFatalEnabled()) {
                        LogUtil.LOG.fatal((Object)("Cannot log with method [" + method + "]"), (Throwable)e);
                    }
                }
                this.reset();
                this.scan = 0;
            }
        }, true);
    }
    
    public static PrintStream getTraceStream(final Logger logger) {
        return getLogStream(logger, LogUtil.LOGGER_TRACE);
    }
    
    public static PrintStream getDebugStream(final Logger logger) {
        return getLogStream(logger, LogUtil.LOGGER_DEBUG);
    }
    
    public static PrintStream getInfoStream(final Logger logger) {
        return getLogStream(logger, LogUtil.LOGGER_INFO);
    }
    
    public static PrintStream getWarnStream(final Logger logger) {
        return getLogStream(logger, LogUtil.LOGGER_WARN);
    }
    
    public static PrintStream getErrorStream(final Logger logger) {
        return getLogStream(logger, LogUtil.LOGGER_ERROR);
    }
    
    private static PrintStream getLogStream(final Logger logger, final Method method) {
        return new PrintStream(new ByteArrayOutputStream() {
            private int scan = 0;
            
            private boolean hasNewline() {
                while (this.scan < this.count) {
                    if (this.buf[this.scan] == 10) {
                        return true;
                    }
                    ++this.scan;
                }
                return false;
            }
            
            @Override
            public void flush() throws IOException {
                if (!this.hasNewline()) {
                    return;
                }
                try {
                    method.invoke(logger, this.toString().trim());
                }
                catch (Exception e) {
                    if (LogUtil.LOG.isErrorEnabled()) {
                        LogUtil.LOG.error((Object)("Cannot log with method [" + method + "]"), (Throwable)e);
                    }
                }
                this.reset();
                this.scan = 0;
            }
        }, true);
    }
    
    static {
        LOG = LogFactory.getLog((Class)LogUtil.class);
        LOG_CACHE = new HashMap<Class<?>, Log>();
        LogUtil.LOG_TRACE = null;
        LogUtil.LOG_DEBUG = null;
        LogUtil.LOG_INFO = null;
        LogUtil.LOG_WARN = null;
        LogUtil.LOG_ERROR = null;
        LogUtil.LOG_FATAL = null;
        LogUtil.LOGGER_TRACE = null;
        LogUtil.LOGGER_DEBUG = null;
        LogUtil.LOGGER_INFO = null;
        LogUtil.LOGGER_WARN = null;
        LogUtil.LOGGER_ERROR = null;
        try {
            LogUtil.LOG_TRACE = Log.class.getMethod("trace", Object.class);
            LogUtil.LOG_DEBUG = Log.class.getMethod("debug", Object.class);
            LogUtil.LOG_INFO = Log.class.getMethod("info", Object.class);
            LogUtil.LOG_WARN = Log.class.getMethod("warn", Object.class);
            LogUtil.LOG_ERROR = Log.class.getMethod("error", Object.class);
            LogUtil.LOG_FATAL = Log.class.getMethod("fatal", Object.class);
            LogUtil.LOGGER_TRACE = Logger.class.getMethod("trace", String.class);
            LogUtil.LOGGER_DEBUG = Logger.class.getMethod("debug", String.class);
            LogUtil.LOGGER_INFO = Logger.class.getMethod("info", String.class);
            LogUtil.LOGGER_WARN = Logger.class.getMethod("warn", String.class);
            LogUtil.LOGGER_ERROR = Logger.class.getMethod("error", String.class);
        }
        catch (Exception e) {
            if (LogUtil.LOG.isErrorEnabled()) {
                LogUtil.LOG.error((Object)"Cannot init log methods", (Throwable)e);
            }
        }
    }
}
