package com.dataliance.dom;

import org.w3c.dom.*;
import java.util.*;

public class NodeWalker
{
    private Node currentNode;
    private NodeList currentChildren;
    private Stack<Node> nodes;
    
    public NodeWalker(final Node rootNode) {
        (this.nodes = new Stack<Node>()).add(rootNode);
    }
    
    public Node nextNode() {
        if (!this.hasNext()) {
            return null;
        }
        this.currentNode = this.nodes.pop();
        this.currentChildren = this.currentNode.getChildNodes();
        final int childLen = (this.currentChildren != null) ? this.currentChildren.getLength() : 0;
        for (int i = childLen - 1; i >= 0; --i) {
            this.nodes.add(this.currentChildren.item(i));
        }
        return this.currentNode;
    }
    
    public void skipChildren() {
        for (int childLen = (this.currentChildren != null) ? this.currentChildren.getLength() : 0, i = 0; i < childLen; ++i) {
            final Node child = this.nodes.peek();
            if (child.equals(this.currentChildren.item(i))) {
                this.nodes.pop();
            }
        }
    }
    
    public boolean hasNext() {
        return this.nodes.size() > 0;
    }
}
