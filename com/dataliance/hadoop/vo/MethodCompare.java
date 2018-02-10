package com.dataliance.hadoop.vo;

class MethodCompare implements Comparable<MethodCompare>
{
    private int num;
    private Object value;
    private Class<?> type;
    
    public MethodCompare() {
    }
    
    public MethodCompare(final int num, final Object value) {
        this.num = num;
        this.value = value;
    }
    
    public MethodCompare(final int num, final Class<?> type) {
        this.num = num;
        this.type = type;
    }
    
    public Class<?> getType() {
        return this.type;
    }
    
    public void setType(final Class<?> type) {
        this.type = type;
    }
    
    public int getNum() {
        return this.num;
    }
    
    public void setNum(final int num) {
        this.num = num;
    }
    
    public Object getValue() {
        return this.value;
    }
    
    public void setValue(final Object value) {
        this.value = value;
    }
    
    @Override
    public int compareTo(final MethodCompare o) {
        return this.num - o.num;
    }
}
