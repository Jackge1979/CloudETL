package com.dataliance.nutch.segment;

import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class WebDocument implements Writable
{
    private String url;
    private String title;
    private String content;
    private String tokens;
    private String category;
    private String termSplitTag;
    
    public WebDocument() {
        this.url = "";
        this.title = "";
        this.content = "";
        this.tokens = "";
        this.category = "unknow";
        this.termSplitTag = " ";
    }
    
    public WebDocument(final String url, final String title, final String content, final String termSplitTag) {
        this(url, title, content, "", termSplitTag);
    }
    
    public WebDocument(final String url, final String title, final String content, final String tokens, final String termSplitTag) {
        this(url, title, content, tokens, termSplitTag, "unknow");
    }
    
    public WebDocument(final String url, final String title, final String content, final String tokens, final String termSplitTag, final String category) {
        this.url = "";
        this.title = "";
        this.content = "";
        this.tokens = "";
        this.category = "unknow";
        this.termSplitTag = " ";
        this.url = url;
        this.title = title;
        this.content = content;
        this.tokens = tokens;
        this.termSplitTag = termSplitTag;
        this.category = category;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public void setUrl(final String url) {
        this.url = url;
    }
    
    public String getTitle() {
        return this.title;
    }
    
    public void setTitle(final String title) {
        this.title = title;
    }
    
    public String getContent() {
        return this.content;
    }
    
    public void setContent(final String content) {
        this.content = content;
    }
    
    public String getCategory() {
        return this.category;
    }
    
    public void setCategory(final String category) {
        this.category = category;
    }
    
    public void write(final DataOutput out) throws IOException {
        Text.writeString(out, this.url);
        Text.writeString(out, this.title);
        Text.writeString(out, this.content);
        Text.writeString(out, this.tokens);
        Text.writeString(out, this.termSplitTag);
        Text.writeString(out, this.category);
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.url = Text.readString(in);
        this.title = Text.readString(in);
        this.content = Text.readString(in);
        this.tokens = Text.readString(in);
        this.termSplitTag = Text.readString(in);
        this.category = Text.readString(in);
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof WebDocument)) {
            return false;
        }
        final WebDocument other = (WebDocument)o;
        return this.url.equals(other.url) && this.title.equals(other.title) && this.category.equals(other.category);
    }
    
    @Override
    public String toString() {
        return String.format("url:%s, title:%s, category:%s, tokens:%s, content:%s", this.url, this.title, this.category, this.tokens, this.content);
    }
    
    public String getTokens() {
        return this.tokens;
    }
    
    public void setTokens(final String tokens) {
        this.tokens = tokens;
    }
    
    public String getTermSplitTag() {
        return this.termSplitTag;
    }
    
    public void setTermSplitTag(final String termSplitTag) {
        this.termSplitTag = termSplitTag;
    }
    
    public static void main(final String[] args) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        final FileSystem fs = FileSystem.get(conf);
        final Path path = new Path("./doc");
        final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, (Class)NullWritable.class, (Class)WebDocument.class);
        writer.append((Writable)NullWritable.get(), (Writable)new WebDocument("url", "title", "content", "", " "));
        writer.close();
    }
}
