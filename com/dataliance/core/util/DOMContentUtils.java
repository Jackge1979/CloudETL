package com.dataliance.core.util;

import org.apache.hadoop.conf.*;
import java.util.*;
import com.dataliance.dom.*;
import java.net.*;
import org.w3c.dom.*;
import com.dataliance.dom.parse.*;

public class DOMContentUtils
{
    private HashMap linkParams;
    private Configuration conf;
    
    public DOMContentUtils(final Configuration conf) {
        this.linkParams = new HashMap();
        this.conf = conf;
    }
    
    public void setConf(final Configuration conf) {
        final Collection<String> forceTags = new ArrayList<String>(1);
        this.linkParams.clear();
        this.linkParams.put("a", new LinkParams("a", "href", 1));
        this.linkParams.put("area", new LinkParams("area", "href", 0));
        if (conf.getBoolean("parser.html.form.use_action", true)) {
            this.linkParams.put("form", new LinkParams("form", "action", 1));
            if (conf.get("parser.html.form.use_action") != null) {
                forceTags.add("form");
            }
        }
        this.linkParams.put("frame", new LinkParams("frame", "src", 0));
        this.linkParams.put("iframe", new LinkParams("iframe", "src", 0));
        this.linkParams.put("script", new LinkParams("script", "src", 0));
        this.linkParams.put("link", new LinkParams("link", "href", 0));
        this.linkParams.put("img", new LinkParams("img", "src", 0));
        final String[] ignoreTags = conf.getStrings("parser.html.outlinks.ignore_tags");
        for (int i = 0; ignoreTags != null && i < ignoreTags.length; ++i) {
            if (!forceTags.contains(ignoreTags[i])) {
                this.linkParams.remove(ignoreTags[i]);
            }
        }
    }
    
    public boolean getText(final StringBuffer sb, final Node node, final boolean abortOnNestedAnchors) {
        return this.getTextHelper(sb, node, abortOnNestedAnchors, 0);
    }
    
    public void getText(final StringBuffer sb, final Node node) {
        this.getText(sb, node, false);
    }
    
    private boolean getTextHelper(final StringBuffer sb, final Node node, final boolean abortOnNestedAnchors, int anchorDepth) {
        boolean abort = false;
        final NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {
            final Node currentNode = walker.nextNode();
            final String nodeName = currentNode.getNodeName();
            final short nodeType = currentNode.getNodeType();
            if ("script".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if ("style".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if (abortOnNestedAnchors && "a".equalsIgnoreCase(nodeName) && ++anchorDepth > 1) {
                abort = true;
                break;
            }
            if (nodeType == 8) {
                walker.skipChildren();
            }
            if (nodeType != 3) {
                continue;
            }
            String text = currentNode.getNodeValue();
            text = text.replaceAll("\\s+", " ");
            text = text.trim();
            if (text.length() <= 0) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(text);
        }
        return abort;
    }
    
    public boolean getTitle(final StringBuffer sb, final Node node) {
        final NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {
            final Node currentNode = walker.nextNode();
            final String nodeName = currentNode.getNodeName();
            final short nodeType = currentNode.getNodeType();
            if ("body".equalsIgnoreCase(nodeName)) {
                return false;
            }
            if (nodeType == 1 && "title".equalsIgnoreCase(nodeName)) {
                this.getText(sb, currentNode);
                return true;
            }
        }
        return false;
    }
    
    public URL getBase(final Node node) {
        final NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {
            final Node currentNode = walker.nextNode();
            final String nodeName = currentNode.getNodeName();
            final short nodeType = currentNode.getNodeType();
            if (nodeType == 1) {
                if ("body".equalsIgnoreCase(nodeName)) {
                    return null;
                }
                if (!"base".equalsIgnoreCase(nodeName)) {
                    continue;
                }
                final NamedNodeMap attrs = currentNode.getAttributes();
                for (int i = 0; i < attrs.getLength(); ++i) {
                    final Node attr = attrs.item(i);
                    if ("href".equalsIgnoreCase(attr.getNodeName())) {
                        try {
                            return new URL(attr.getNodeValue());
                        }
                        catch (MalformedURLException ex) {}
                    }
                }
            }
        }
        return null;
    }
    
    private boolean hasOnlyWhiteSpace(final Node node) {
        final String val = node.getNodeValue();
        for (int i = 0; i < val.length(); ++i) {
            if (!Character.isWhitespace(val.charAt(i))) {
                return false;
            }
        }
        return true;
    }
    
    private boolean shouldThrowAwayLink(final Node node, final NodeList children, final int childLen, final LinkParams params) {
        if (childLen == 0) {
            return params.childLen != 0;
        }
        if (childLen == 1 && children.item(0).getNodeType() == 1 && params.elName.equalsIgnoreCase(children.item(0).getNodeName())) {
            return true;
        }
        if (childLen == 2) {
            final Node c0 = children.item(0);
            final Node c2 = children.item(1);
            if (c0.getNodeType() == 1 && params.elName.equalsIgnoreCase(c0.getNodeName()) && c2.getNodeType() == 3 && this.hasOnlyWhiteSpace(c2)) {
                return true;
            }
            if (c2.getNodeType() == 1 && params.elName.equalsIgnoreCase(c2.getNodeName()) && c0.getNodeType() == 3 && this.hasOnlyWhiteSpace(c0)) {
                return true;
            }
        }
        else if (childLen == 3) {
            final Node c0 = children.item(0);
            final Node c2 = children.item(1);
            final Node c3 = children.item(2);
            if (c2.getNodeType() == 1 && params.elName.equalsIgnoreCase(c2.getNodeName()) && c0.getNodeType() == 3 && c3.getNodeType() == 3 && this.hasOnlyWhiteSpace(c0) && this.hasOnlyWhiteSpace(c3)) {
                return true;
            }
        }
        return false;
    }
    
    private URL fixEmbeddedParams(final URL base, String target) throws MalformedURLException {
        if (target.indexOf(59) >= 0 || base.toString().indexOf(59) == -1) {
            return new URL(base, target);
        }
        final String baseURL = base.toString();
        final int startParams = baseURL.indexOf(59);
        final String params = baseURL.substring(startParams);
        final int startQS = target.indexOf(63);
        if (startQS >= 0) {
            target = target.substring(0, startQS) + params + target.substring(startQS);
        }
        else {
            target += params;
        }
        return new URL(base, target);
    }
    
    public void getOutlinks(final URL base, final ArrayList outlinks, final Node node) {
        final NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {
            final Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            final short nodeType = currentNode.getNodeType();
            final NodeList children = currentNode.getChildNodes();
            final int childLen = (children != null) ? children.getLength() : 0;
            if (nodeType == 1) {
                nodeName = nodeName.toLowerCase();
                final LinkParams params = this.linkParams.get(nodeName);
                if (params == null) {
                    continue;
                }
                if (!this.shouldThrowAwayLink(currentNode, children, childLen, params)) {
                    final StringBuffer linkText = new StringBuffer();
                    this.getText(linkText, currentNode, true);
                    final NamedNodeMap attrs = currentNode.getAttributes();
                    String target = null;
                    boolean noFollow = false;
                    boolean post = false;
                    for (int i = 0; i < attrs.getLength(); ++i) {
                        final Node attr = attrs.item(i);
                        final String attrName = attr.getNodeName();
                        if (params.attrName.equalsIgnoreCase(attrName)) {
                            target = attr.getNodeValue();
                        }
                        else if ("rel".equalsIgnoreCase(attrName) && "nofollow".equalsIgnoreCase(attr.getNodeValue())) {
                            noFollow = true;
                        }
                        else if ("method".equalsIgnoreCase(attrName) && "post".equalsIgnoreCase(attr.getNodeValue())) {
                            post = true;
                        }
                    }
                    if (target != null && !noFollow && !post) {
                        try {
                            final URL url = (base.toString().indexOf(59) > 0) ? this.fixEmbeddedParams(base, target) : new URL(base, target);
                            outlinks.add(new Outlink(url.toString(), linkText.toString().trim()));
                        }
                        catch (MalformedURLException ex) {}
                    }
                }
                if (params.childLen == 0) {
                    continue;
                }
                continue;
            }
        }
    }
    
    public static class LinkParams
    {
        public String elName;
        public String attrName;
        public int childLen;
        
        public LinkParams(final String elName, final String attrName, final int childLen) {
            this.elName = elName;
            this.attrName = attrName;
            this.childLen = childLen;
        }
        
        @Override
        public String toString() {
            return "LP[el=" + this.elName + ",attr=" + this.attrName + ",len=" + this.childLen + "]";
        }
    }
}
