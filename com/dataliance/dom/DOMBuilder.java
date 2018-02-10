package com.dataliance.dom;

import org.xml.sax.ext.*;
import java.util.*;
import java.io.*;
import org.xml.sax.*;
import com.sun.org.apache.xml.internal.utils.*;
import org.w3c.dom.*;

public class DOMBuilder implements ContentHandler, LexicalHandler
{
    public Document m_doc;
    protected Node m_currentNode;
    public DocumentFragment m_docFrag;
    protected Stack m_elemStack;
    protected boolean m_inCData;
    
    public DOMBuilder(final Document doc, final Node node) {
        this.m_currentNode = null;
        this.m_docFrag = null;
        this.m_elemStack = new Stack();
        this.m_inCData = false;
        this.m_doc = doc;
        this.m_currentNode = node;
    }
    
    public DOMBuilder(final Document doc, final DocumentFragment docFrag) {
        this.m_currentNode = null;
        this.m_docFrag = null;
        this.m_elemStack = new Stack();
        this.m_inCData = false;
        this.m_doc = doc;
        this.m_docFrag = docFrag;
    }
    
    public DOMBuilder(final Document doc) {
        this.m_currentNode = null;
        this.m_docFrag = null;
        this.m_elemStack = new Stack();
        this.m_inCData = false;
        this.m_doc = doc;
    }
    
    public Node getRootNode() {
        return (Node)((null != this.m_docFrag) ? this.m_docFrag : this.m_doc);
    }
    
    public Node getCurrentNode() {
        return this.m_currentNode;
    }
    
    public Writer getWriter() {
        return null;
    }
    
    protected void append(final Node newNode) throws SAXException {
        final Node currentNode = this.m_currentNode;
        if (null != currentNode) {
            currentNode.appendChild(newNode);
        }
        else if (null != this.m_docFrag) {
            this.m_docFrag.appendChild(newNode);
        }
        else {
            boolean ok = true;
            final short type = newNode.getNodeType();
            if (type == 3) {
                final String data = newNode.getNodeValue();
                if (null != data && data.trim().length() > 0) {
                    throw new SAXException("Warning: can't output text before document element!  Ignoring...");
                }
                ok = false;
            }
            else if (type == 1 && this.m_doc.getDocumentElement() != null) {
                throw new SAXException("Can't have more than one root on a DOM!");
            }
            if (ok) {
                this.m_doc.appendChild(newNode);
            }
        }
    }
    
    @Override
    public void setDocumentLocator(final Locator locator) {
    }
    
    @Override
    public void startDocument() throws SAXException {
    }
    
    @Override
    public void endDocument() throws SAXException {
    }
    
    @Override
    public void startElement(final String ns, final String localName, final String name, final Attributes atts) throws SAXException {
        Element elem;
        if (null == ns || ns.length() == 0) {
            elem = this.m_doc.createElementNS(null, name);
        }
        else {
            elem = this.m_doc.createElementNS(ns, name);
        }
        this.append(elem);
        try {
            final int nAtts = atts.getLength();
            if (0 != nAtts) {
                for (int i = 0; i < nAtts; ++i) {
                    if (atts.getType(i).equalsIgnoreCase("ID")) {
                        this.setIDAttribute(atts.getValue(i), elem);
                    }
                    String attrNS = atts.getURI(i);
                    if ("".equals(attrNS)) {
                        attrNS = null;
                    }
                    final String attrQName = atts.getQName(i);
                    if (attrQName.startsWith("xmlns:")) {
                        attrNS = "http://www.w3.org/2000/xmlns/";
                    }
                    elem.setAttributeNS(attrNS, attrQName, atts.getValue(i));
                }
            }
            this.m_elemStack.push(elem);
            this.m_currentNode = elem;
        }
        catch (Exception de) {
            throw new SAXException(de);
        }
    }
    
    @Override
    public void endElement(final String ns, final String localName, final String name) throws SAXException {
        this.m_elemStack.pop();
        this.m_currentNode = (this.m_elemStack.isEmpty() ? null : this.m_elemStack.peek());
    }
    
    public void setIDAttribute(final String id, final Element elem) {
    }
    
    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        if (this.isOutsideDocElem() && XMLCharacterRecognizer.isWhiteSpace(ch, start, length)) {
            return;
        }
        if (this.m_inCData) {
            this.cdata(ch, start, length);
            return;
        }
        final String s = new String(ch, start, length);
        final Node childNode = (this.m_currentNode != null) ? this.m_currentNode.getLastChild() : null;
        if (childNode != null && childNode.getNodeType() == 3) {
            ((Text)childNode).appendData(s);
        }
        else {
            final Text text = this.m_doc.createTextNode(s);
            this.append(text);
        }
    }
    
    public void charactersRaw(final char[] ch, final int start, final int length) throws SAXException {
        if (this.isOutsideDocElem() && XMLCharacterRecognizer.isWhiteSpace(ch, start, length)) {
            return;
        }
        final String s = new String(ch, start, length);
        this.append(this.m_doc.createProcessingInstruction("xslt-next-is-raw", "formatter-to-dom"));
        this.append(this.m_doc.createTextNode(s));
    }
    
    @Override
    public void startEntity(final String name) throws SAXException {
    }
    
    @Override
    public void endEntity(final String name) throws SAXException {
    }
    
    public void entityReference(final String name) throws SAXException {
        this.append(this.m_doc.createEntityReference(name));
    }
    
    @Override
    public void ignorableWhitespace(final char[] ch, final int start, final int length) throws SAXException {
        if (this.isOutsideDocElem()) {
            return;
        }
        final String s = new String(ch, start, length);
        this.append(this.m_doc.createTextNode(s));
    }
    
    private boolean isOutsideDocElem() {
        return null == this.m_docFrag && this.m_elemStack.size() == 0 && (null == this.m_currentNode || this.m_currentNode.getNodeType() == 9);
    }
    
    @Override
    public void processingInstruction(final String target, final String data) throws SAXException {
        this.append(this.m_doc.createProcessingInstruction(target, data));
    }
    
    @Override
    public void comment(final char[] ch, final int start, final int length) throws SAXException {
        if (ch == null || start < 0 || length >= ch.length - start || length < 0) {
            return;
        }
        this.append(this.m_doc.createComment(new String(ch, start, length)));
    }
    
    @Override
    public void startCDATA() throws SAXException {
        this.m_inCData = true;
        this.append(this.m_doc.createCDATASection(""));
    }
    
    @Override
    public void endCDATA() throws SAXException {
        this.m_inCData = false;
    }
    
    public void cdata(final char[] ch, final int start, final int length) throws SAXException {
        if (this.isOutsideDocElem() && XMLCharacterRecognizer.isWhiteSpace(ch, start, length)) {
            return;
        }
        final String s = new String(ch, start, length);
        final Node n = this.m_currentNode.getLastChild();
        if (n instanceof CDATASection) {
            ((CDATASection)n).appendData(s);
        }
        else if (n instanceof Comment) {
            ((Comment)n).appendData(s);
        }
    }
    
    @Override
    public void startDTD(final String name, final String publicId, final String systemId) throws SAXException {
    }
    
    @Override
    public void endDTD() throws SAXException {
    }
    
    @Override
    public void startPrefixMapping(final String prefix, final String uri) throws SAXException {
    }
    
    @Override
    public void endPrefixMapping(final String prefix) throws SAXException {
    }
    
    @Override
    public void skippedEntity(final String name) throws SAXException {
    }
}
