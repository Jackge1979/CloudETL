package com.dataliance.dom.meta;

import com.dataliance.dom.*;
import java.util.*;

public class Metadata implements Mobier, HttpHeaders, CreativeCommons, DublinCore, Office, Feed
{
    private Map<String, String[]> metadata;
    
    public Metadata() {
        this.metadata = null;
        this.metadata = new HashMap<String, String[]>();
    }
    
    public boolean isMultiValued(final String name) {
        return this.metadata.get(name) != null && this.metadata.get(name).length > 1;
    }
    
    public String[] names() {
        return this.metadata.keySet().toArray(new String[this.metadata.keySet().size()]);
    }
    
    public String get(final String name) {
        final String[] values = this.metadata.get(name);
        if (values == null) {
            return null;
        }
        return values[0];
    }
    
    public String[] getValues(final String name) {
        return this._getValues(name);
    }
    
    private String[] _getValues(final String name) {
        String[] values = this.metadata.get(name);
        if (values == null) {
            values = new String[0];
        }
        return values;
    }
    
    public void add(final String name, final String value) {
        final String[] values = this.metadata.get(name);
        if (values == null) {
            this.set(name, value);
        }
        else {
            final String[] newValues = new String[values.length + 1];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[newValues.length - 1] = value;
            this.metadata.put(name, newValues);
        }
    }
    
    public void setAll(final Properties properties) {
        final Enumeration names = properties.propertyNames();
        while (names.hasMoreElements()) {
            final String name = names.nextElement();
            this.metadata.put(name, new String[] { properties.getProperty(name) });
        }
    }
    
    public void set(final String name, final String value) {
        this.metadata.put(name, new String[] { value });
    }
    
    public void remove(final String name) {
        this.metadata.remove(name);
    }
    
    public int size() {
        return this.metadata.size();
    }
    
    public void clear() {
        this.metadata.clear();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        Metadata other = null;
        try {
            other = (Metadata)o;
        }
        catch (ClassCastException cce) {
            return false;
        }
        if (other.size() != this.size()) {
            return false;
        }
        final String[] names = this.names();
        for (int i = 0; i < names.length; ++i) {
            final String[] otherValues = other._getValues(names[i]);
            final String[] thisValues = this._getValues(names[i]);
            if (otherValues.length != thisValues.length) {
                return false;
            }
            for (int j = 0; j < otherValues.length; ++j) {
                if (!otherValues[j].equals(thisValues[j])) {
                    return false;
                }
            }
        }
        return true;
    }
    
    @Override
    public String toString() {
        final StringBuffer buf = new StringBuffer();
        final String[] names = this.names();
        for (int i = 0; i < names.length; ++i) {
            final String[] values = this._getValues(names[i]);
            for (int j = 0; j < values.length; ++j) {
                buf.append(names[i]).append("=").append(values[j]).append(" ");
            }
        }
        return buf.toString();
    }
}
