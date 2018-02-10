package com.dataliance.analysis.data.terminal.model;

public class TerminalCountModel
{
    private int num;
    private String terminal;
    
    public int getNum() {
        return this.num;
    }
    
    public void setNum(final int num) {
        this.num = num;
    }
    
    public String getTerminal() {
        return this.terminal;
    }
    
    public void setTerminal(final String terminal) {
        this.terminal = terminal;
    }
    
    @Override
    public String toString() {
        return "TerminalCountModel [num=" + this.num + ", terminal=" + this.terminal + "]";
    }
}
