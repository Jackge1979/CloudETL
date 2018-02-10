package com.dataliance.service.util;

import com.ibatis.common.resources.*;
import com.ibatis.sqlmap.client.*;
import java.io.*;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;

public class IbatisSqlMapClient
{
    private static Map<String, SqlMapClient> name2client;
    
    private static SqlMapClient createNewSqlMapClient(final String name) {
        Reader reader = null;
        try {
            String resource = "sql-map-config.xml";
            if (!"mysql".equals(name)) {
                resource = name + "-sql-map-config.xml";
            }
            reader = Resources.getResourceAsReader(resource);
            return SqlMapClientBuilder.buildSqlMapClient(reader);
        }
        catch (Exception e) {
            e.printStackTrace();
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
        return null;
    }
    
    public static SqlMapClient get() {
        return get("mysql");
    }
    
    public static SqlMapClient get(String name) {
        if ("derby".equals(name)) {
            name = "test/derby";
        }
        if (!IbatisSqlMapClient.name2client.containsKey(name)) {
            IbatisSqlMapClient.name2client.put(name, createNewSqlMapClient(name));
        }
        return IbatisSqlMapClient.name2client.get(name);
    }
    
    public static void initDerby() {
        get("test/derby");
    }
    
    public static void execute(final String statement) {
        final SqlMapClient client = get("derby");
        try {
            final Connection connection = client.getDataSource().getConnection();
            connection.setAutoCommit(true);
            final Statement stmt = connection.createStatement();
            stmt.execute(statement);
            stmt.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static List<Object> extractData(final ResultSet resultSet, final Class cls) {
        final List<Object> objs = new ArrayList<Object>();
        try {
            final Map<String, Method> property2setter = new HashMap<String, Method>();
            for (final Method method : cls.getMethods()) {
                String methodName = method.getName();
                if (methodName.startsWith("set")) {
                    methodName = methodName.substring(3);
                    final String firstLetter = "" + methodName.charAt(0);
                    methodName = firstLetter.toLowerCase() + methodName.substring(1);
                    property2setter.put(methodName, method);
                }
            }
            while (resultSet.next()) {
                final Object obj = cls.newInstance();
                for (final Map.Entry<String, Method> entry : property2setter.entrySet()) {
                    try {
                        final Method setter = entry.getValue();
                        final String property = entry.getKey();
                        setter.invoke(obj, resultSet.getObject(property));
                    }
                    catch (Exception e2) {
                        System.err.println("no property : " + entry.getKey());
                    }
                }
                objs.add(obj);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return objs;
    }
    
    public static List executeQuery(final String statement, final Class cls) {
        List<Object> objs = new ArrayList<Object>();
        try {
            final SqlMapClient client = get("derby");
            final Connection connection = client.getDataSource().getConnection();
            final Statement stmt = connection.createStatement();
            final ResultSet resultSet = stmt.executeQuery(statement);
            objs = extractData(resultSet, cls);
            resultSet.close();
            stmt.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return objs;
    }
    
    public static void shutdownDerby() {
        System.out.println("shut down derby");
        createNewSqlMapClient("test/derby-shutdown");
    }
    
    static {
        IbatisSqlMapClient.name2client = new HashMap<String, SqlMapClient>();
    }
}
