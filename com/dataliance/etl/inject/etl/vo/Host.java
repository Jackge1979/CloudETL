package com.dataliance.etl.inject.etl.vo;
/*
 * 配置用户名和密码
 */
public class Host
{
    private String user;
    private String password;
    private String host;
    private int port;
    private boolean usePassword;
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(final String user) {
        this.user = user;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    public void setPassword(final String password) {
        this.password = password;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public boolean isUsePassword() {
        return this.usePassword;
    }
    
    public void setUsePassword(final boolean usePassword) {
        this.usePassword = usePassword;
    }
    
    @Override
    public String toString() {
        return this.host;
    }
}
