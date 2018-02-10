package com.dataliance.dom;

import org.w3c.dom.*;
import java.util.*;

public class NodeIterator implements Iterator<ParseNode>
{
    private ParseNode currentNode;
    private NodeList currentChildren;
    private Stack<ParseNode> nodes;
    
    public NodeIterator(final ParseNode rootNode) {
        (this.nodes = new Stack<ParseNode>()).add(rootNode);
    }
    
    public void skipChildren() {
        for (int childLen = (this.currentChildren != null) ? this.currentChildren.getLength() : 0, i = 0; i < childLen; ++i) {
            final ParseNode child = this.nodes.peek();
            if (child.equals(this.currentChildren.item(i))) {
                this.nodes.pop();
            }
        }
    }
    
    @Override
    public boolean hasNext() {
        return this.nodes.size() > 0;
    }
    
    @Override
    public ParseNode next() {
        if (!this.hasNext()) {
            return null;
        }
        this.currentNode = this.nodes.pop();
        this.currentChildren = this.currentNode.getChildNodes();
        final int childLen = (this.currentChildren != null) ? this.currentChildren.getLength() : 0;
        for (int i = childLen - 1; i >= 0; --i) {
            this.nodes.add(new ParseNode(this.currentChildren.item(i)));
        }
        return this.currentNode;
    }
    
    @Override
    public void remove() {
        this.nodes.pop();
    }
}
