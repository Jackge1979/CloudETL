package com.dataliance.util;

import java.io.*;
import org.apache.hadoop.util.*;
import org.xml.sax.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

class DomainSuffixesReader
{
    void read(final DomainSuffixes tldEntries, final InputStream input) throws IOException {
        try {
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setIgnoringComments(true);
            final DocumentBuilder builder = factory.newDocumentBuilder();
            final Document document = builder.parse(new InputSource(input));
            final Element root = document.getDocumentElement();
            if (root == null || !root.getTagName().equals("domains")) {
                throw new IOException("xml file is not valid");
            }
            final Element tlds = (Element)root.getElementsByTagName("tlds").item(0);
            final Element suffixes = (Element)root.getElementsByTagName("suffixes").item(0);
            this.readITLDs(tldEntries, (Element)tlds.getElementsByTagName("itlds").item(0));
            this.readGTLDs(tldEntries, (Element)tlds.getElementsByTagName("gtlds").item(0));
            this.readCCTLDs(tldEntries, (Element)tlds.getElementsByTagName("cctlds").item(0));
            this.readSuffixes(tldEntries, suffixes);
        }
        catch (ParserConfigurationException ex) {
            System.out.println(StringUtils.stringifyException((Throwable)ex));
            throw new IOException(ex.getMessage());
        }
        catch (SAXException ex2) {
            System.out.println(StringUtils.stringifyException((Throwable)ex2));
            throw new IOException(ex2.getMessage());
        }
    }
    
    void readITLDs(final DomainSuffixes tldEntries, final Element el) {
        final NodeList children = el.getElementsByTagName("tld");
        for (int i = 0; i < children.getLength(); ++i) {
            tldEntries.addDomainSuffix(this.readGTLD((Element)children.item(i), TopLevelDomain.Type.INFRASTRUCTURE));
        }
    }
    
    void readGTLDs(final DomainSuffixes tldEntries, final Element el) {
        final NodeList children = el.getElementsByTagName("tld");
        for (int i = 0; i < children.getLength(); ++i) {
            tldEntries.addDomainSuffix(this.readGTLD((Element)children.item(i), TopLevelDomain.Type.GENERIC));
        }
    }
    
    void readCCTLDs(final DomainSuffixes tldEntries, final Element el) throws IOException {
        final NodeList children = el.getElementsByTagName("tld");
        for (int i = 0; i < children.getLength(); ++i) {
            tldEntries.addDomainSuffix(this.readCCTLD((Element)children.item(i)));
        }
    }
    
    TopLevelDomain readGTLD(final Element el, final TopLevelDomain.Type type) {
        final String domain = el.getAttribute("domain");
        final DomainSuffix.Status status = this.readStatus(el);
        final float boost = this.readBoost(el);
        return new TopLevelDomain(domain, type, status, boost);
    }
    
    TopLevelDomain readCCTLD(final Element el) throws IOException {
        final String domain = el.getAttribute("domain");
        final DomainSuffix.Status status = this.readStatus(el);
        final float boost = this.readBoost(el);
        final String countryName = this.readCountryName(el);
        return new TopLevelDomain(domain, status, boost, countryName);
    }
    
    DomainSuffix.Status readStatus(final Element el) {
        final NodeList list = el.getElementsByTagName("status");
        if (list == null || list.getLength() == 0) {
            return DomainSuffix.DEFAULT_STATUS;
        }
        return DomainSuffix.Status.valueOf(list.item(0).getFirstChild().getNodeValue());
    }
    
    float readBoost(final Element el) {
        final NodeList list = el.getElementsByTagName("boost");
        if (list == null || list.getLength() == 0) {
            return 1.0f;
        }
        return Float.parseFloat(list.item(0).getFirstChild().getNodeValue());
    }
    
    String readCountryName(final Element el) throws IOException {
        final NodeList list = el.getElementsByTagName("country");
        if (list == null || list.getLength() == 0) {
            throw new IOException("Country name should be given");
        }
        return list.item(0).getNodeValue();
    }
    
    void readSuffixes(final DomainSuffixes tldEntries, final Element el) {
        final NodeList children = el.getElementsByTagName("suffix");
        for (int i = 0; i < children.getLength(); ++i) {
            tldEntries.addDomainSuffix(this.readSuffix((Element)children.item(i)));
        }
    }
    
    DomainSuffix readSuffix(final Element el) {
        final String domain = el.getAttribute("domain");
        final DomainSuffix.Status status = this.readStatus(el);
        final float boost = this.readBoost(el);
        return new DomainSuffix(domain, status, boost);
    }
}
