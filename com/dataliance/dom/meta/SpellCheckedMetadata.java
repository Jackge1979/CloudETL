package com.dataliance.dom.meta;

import org.apache.commons.lang.*;
import java.util.*;
import com.dataliance.dom.*;
import java.lang.reflect.*;

public class SpellCheckedMetadata extends Metadata
{
    private static final int TRESHOLD_DIVIDER = 3;
    private static final Map<String, String> NAMES_IDX;
    private static String[] normalized;
    
    private static String normalize(final String str) {
        final StringBuffer buf = new StringBuffer();
        for (int i = 0; i < str.length(); ++i) {
            final char c = str.charAt(i);
            if (Character.isLetter(c)) {
                buf.append(Character.toLowerCase(c));
            }
        }
        return buf.toString();
    }
    
    public static String getNormalizedName(final String name) {
        final String searched = normalize(name);
        String value = SpellCheckedMetadata.NAMES_IDX.get(searched);
        if (value == null && SpellCheckedMetadata.normalized != null) {
            final int threshold = searched.length() / 3;
            for (int i = 0; i < SpellCheckedMetadata.normalized.length && value == null; ++i) {
                if (StringUtils.getLevenshteinDistance(searched, SpellCheckedMetadata.normalized[i]) < threshold) {
                    value = SpellCheckedMetadata.NAMES_IDX.get(SpellCheckedMetadata.normalized[i]);
                }
            }
        }
        return (value != null) ? value : name;
    }
    
    @Override
    public void remove(final String name) {
        super.remove(getNormalizedName(name));
    }
    
    @Override
    public void add(final String name, final String value) {
        super.add(getNormalizedName(name), value);
    }
    
    @Override
    public String[] getValues(final String name) {
        return super.getValues(getNormalizedName(name));
    }
    
    @Override
    public String get(final String name) {
        return super.get(getNormalizedName(name));
    }
    
    @Override
    public void set(final String name, final String value) {
        super.set(getNormalizedName(name), value);
    }
    
    static {
        NAMES_IDX = new HashMap<String, String>();
        SpellCheckedMetadata.normalized = null;
        final Class[] arr$;
        final Class[] spellthese = arr$ = new Class[] { HttpHeaders.class };
        for (final Class spellCheckedNames : arr$) {
            for (final Field field : spellCheckedNames.getFields()) {
                final int mods = field.getModifiers();
                if (Modifier.isFinal(mods) && Modifier.isPublic(mods) && Modifier.isStatic(mods) && field.getType().equals(String.class)) {
                    try {
                        final String val = (String)field.get(null);
                        SpellCheckedMetadata.NAMES_IDX.put(normalize(val), val);
                    }
                    catch (Exception ex) {}
                }
            }
        }
        SpellCheckedMetadata.normalized = SpellCheckedMetadata.NAMES_IDX.keySet().toArray(new String[SpellCheckedMetadata.NAMES_IDX.size()]);
    }
}
