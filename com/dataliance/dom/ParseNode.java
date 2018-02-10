package com.dataliance.dom;

import org.w3c.dom.*;
import java.util.*;

public class ParseNode
{
    private Document doc;
    private Node root;
    
    public ParseNode(final Node doc) {
        if (doc instanceof Document) {
            this.doc = (Document)doc;
            this.root = this.doc.getDocumentElement();
        }
        else {
            this.root = doc;
        }
    }
    
    public ParseNode getParent() {
        return new ParseNode(this.root.getParentNode());
    }
    
    public NodeList getChildNodes() {
        return this.root.getChildNodes();
    }
    
    public Iterator<ParseNode> iterator() {
        return new NodeIterator(this);
    }
    
    public boolean hasAttributes() {
        return this.root.hasAttributes();
    }
    
    public boolean hasChildNodes() {
        return this.root.hasChildNodes();
    }
    
    public String getName() {
        if (this.root instanceof Element) {
            return ((Element)this.root).getTagName();
        }
        return "";
    }
    
    public String getValue() {
        return this.root.getTextContent();
    }
    
    public String getLastValue() {
        return this.root.getLastChild().getTextContent();
    }
    
    public String getIndexValue(final int index) {
        if (this.root.hasChildNodes()) {
            final NodeList childs = this.root.getChildNodes();
            if (childs.getLength() > index) {
                final Node item = childs.item(index);
                return item.getTextContent();
            }
        }
        return null;
    }
    
    public String getFirstValue() {
        return this.root.getFirstChild().getTextContent();
    }
    
    public Node getNode() {
        return this.root;
    }
    
    public List<ParseNode> getParseNodes(final String tagName) {
        if (tagName == null) {
            return null;
        }
        final List<ParseNode> lpn = new LinkedList<ParseNode>();
        for (final ParseNode pn : this) {
            if (tagName.equals(pn.getName())) {
                lpn.add(pn);
            }
        }
        return (lpn.size() > 0) ? lpn : null;
    }
    
    public List<ParseNode> getFirstChildParseNodes(final String tagName) {
        if (tagName == null) {
            return null;
        }
        if (this.hasChildNodes()) {
            final List<ParseNode> lpn = new LinkedList<ParseNode>();
            final NodeList nodeList = this.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); ++i) {
                final ParseNode pn = new ParseNode(nodeList.item(i));
                if (tagName.equals(pn.getName())) {
                    lpn.add(pn);
                }
            }
            return (lpn.size() > 0) ? lpn : null;
        }
        return null;
    }
    
    public ParseNode getFirstParseNodeByteAtr(final String atrName, final String value) {
        for (final ParseNode pn : this) {
            if (value.equalsIgnoreCase(pn.getAttribute(atrName))) {
                return pn;
            }
        }
        return null;
    }
    
    public List<ParseNode> getParseNodeByteAtr(final String atrName, final String value) {
        final List<ParseNode> lpn = new LinkedList<ParseNode>();
        for (final ParseNode pn : this) {
            if (value.equals(pn.getAttribute(atrName))) {
                lpn.add(pn);
            }
        }
        return (lpn.size() > 0) ? lpn : null;
    }
    
    public List<ParseNode> getParseNodeStartWithAtr(final String atrName, final String value) {
        final List<ParseNode> lpn = new LinkedList<ParseNode>();
        for (final ParseNode pn : this) {
            final String attr = pn.getAttribute(atrName);
            if (attr != null && attr.startsWith(value)) {
                lpn.add(pn);
            }
        }
        return (lpn.size() > 0) ? lpn : null;
    }
    
    public List<ParseNode> getParseNodeEndWithAtr(final String atrName, final String value) {
        final List<ParseNode> lpn = new LinkedList<ParseNode>();
        for (final ParseNode pn : this) {
            final String attr = pn.getAttribute(atrName);
            if (attr != null && attr.endsWith(value)) {
                lpn.add(pn);
            }
        }
        return (lpn.size() > 0) ? lpn : null;
    }
    
    public List<ParseNode> getParseNodeRegAtr(final String atrName, final String value) {
        final List<ParseNode> lpn = new LinkedList<ParseNode>();
        for (final ParseNode pn : this) {
            final String attr = pn.getAttribute(atrName);
            if (attr != null && attr.matches(value)) {
                lpn.add(pn);
            }
        }
        return (lpn.size() > 0) ? lpn : null;
    }
    
    public ParseNode getParseNode(final String tagName) {
        for (final ParseNode pn : this) {
            if (tagName.equalsIgnoreCase(pn.getName())) {
                return pn;
            }
        }
        return null;
    }
    
    public ParseNode getLastParseNode(final String tagName) {
        ParseNode lastPn = null;
        for (final ParseNode pn : this) {
            if (tagName.equalsIgnoreCase(pn.getName())) {
                lastPn = pn;
            }
        }
        return lastPn;
    }
    
    public String getParseNodeValue(final String tagName) {
        final ParseNode pn = this.getParseNode(tagName);
        return (pn != null) ? pn.getValue() : null;
    }
    
    public String getAttribute(final String attributeName) {
        if (this.hasAttributes()) {
            final Node n = this.root.getAttributes().getNamedItem(attributeName);
            return (n == null) ? null : n.getNodeValue();
        }
        return null;
    }
    
    public Document getDoc() {
        return this.doc;
    }
    
    public void setDoc(final Document doc) {
        this.doc = doc;
    }
}
