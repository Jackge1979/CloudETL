package com.dataliance.dom.protocol;

import com.dataliance.dom.meta.*;
import org.w3c.dom.*;
import net.sf.json.*;

public class Content
{
    private Object root;
    private Metadata metadata;
    
    public Content() {
    }
    
    public Content(final Object root, final Metadata metadata) {
        this.root = root;
        this.metadata = metadata;
    }
    
    public Node getRoot() {
        if (this.root instanceof Node) {
            return (Node)this.root;
        }
        return null;
    }
    
    public JSONArray getJSONArray() {
        if (this.root instanceof JSONArray) {
            return (JSONArray)this.root;
        }
        return null;
    }
    
    public JSONObject getJSONObject() {
        if (this.root instanceof JSONObject) {
            return (JSONObject)this.root;
        }
        return null;
    }
    
    public void setRoot(final Object root) {
        this.root = root;
    }
    
    public Metadata getMetadata() {
        return this.metadata;
    }
    
    public void setMetadata(final Metadata metadata) {
        this.metadata = metadata;
    }
}
