package com.dataliance.dom.parse;

import java.net.*;

public class Outlink
{
    private String toUrl;
    private String anchor;
    
    public Outlink() {
    }
    
    public Outlink(final String toUrl, String anchor) throws MalformedURLException {
        this.toUrl = toUrl;
        if (anchor == null) {
            anchor = "";
        }
        this.anchor = anchor;
    }
    
    public String getToUrl() {
        return this.toUrl;
    }
    
    public String getAnchor() {
        return this.anchor;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Outlink)) {
            return false;
        }
        final Outlink other = (Outlink)o;
        return this.toUrl.equals(other.toUrl) && this.anchor.equals(other.anchor);
    }
    
    @Override
    public String toString() {
        return "toUrl: " + this.toUrl + " anchor: " + this.anchor;
    }
}
