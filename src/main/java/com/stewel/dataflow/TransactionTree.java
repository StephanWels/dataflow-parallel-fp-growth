package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@DefaultCoder(AvroCoder.class)
public final class TransactionTree implements Iterable<ItemsListWithSupport>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionTree.class);

    private static final int DEFAULT_CHILDREN_INITIAL_SIZE = 2;
    private static final int DEFAULT_INITIAL_SIZE = 8;
    private static final float GROWTH_RATE = 1.5f;
    private static final int ROOTNODEID = 0;

    private int[] attribute;
    private int[] childCount;
    private int[][] nodeChildren;
    private long[] nodeCount;
    private int nodes;
    private boolean representedAsList;
    private List<ItemsListWithSupport> transactionSet;

    public TransactionTree() {
        this(DEFAULT_INITIAL_SIZE);
    }

    public TransactionTree(int size) {
        if (size < DEFAULT_INITIAL_SIZE) {
            size = DEFAULT_INITIAL_SIZE;
        }
        childCount = new int[size];
        attribute = new int[size];
        nodeCount = new long[size];
        nodeChildren = new int[size][];
        createRootNode();
        representedAsList = false;
    }

    public TransactionTree(final int[] items, final Long support) {
        representedAsList = true;
        transactionSet = Lists.newArrayList();
        transactionSet.add(new ItemsListWithSupport(items, support));
    }

    public TransactionTree(final List<ItemsListWithSupport> transactionSet) {
        representedAsList = true;
        this.transactionSet = transactionSet;
    }

    public void addChild(final int parentNodeId, final int childnodeId) {
        int length = childCount[parentNodeId];
        if (length >= nodeChildren[parentNodeId].length) {
            resizeChildren(parentNodeId);
        }
        nodeChildren[parentNodeId][length++] = childnodeId;
        childCount[parentNodeId] = length;
    }

    public boolean addCount(final int nodeId, final long nextNodeCount) {
        if (nodeId < nodes) {
            this.nodeCount[nodeId] += nextNodeCount;
            return true;
        }
        return false;
    }

    public int addPattern(final int[] myList, final long addCount) {
        int temp = ROOTNODEID;
        int ret = 0;
        boolean addCountMode = true;
        for (final Integer aMyList : myList) {
            final int attributeValue = aMyList;
            int child;
            if (addCountMode) {
                child = childWithAttribute(temp, attributeValue);
                if (child == -1) {
                    addCountMode = false;
                } else {
                    addCount(child, addCount);
                    temp = child;
                }
            }
            if (!addCountMode) {
                child = createNode(temp, attributeValue, addCount);
                temp = child;
                ret++;
            }
        }
        return ret;
    }

    public int attribute(final int nodeId) {
        return this.attribute[nodeId];
    }

    public int childAtIndex(final int nodeId, final int index) {
        if (childCount[nodeId] < index) {
            return -1;
        }
        return nodeChildren[nodeId][index];
    }

    public int childCount() {
        int sum = 0;
        for (int i = 0; i < nodes; i++) {
            sum += childCount[i];
        }
        return sum;
    }

    public int childCount(final int nodeId) {
        return childCount[nodeId];
    }

    public int childWithAttribute(final int nodeId, final int childAttribute) {
        final int length = childCount[nodeId];
        for (int i = 0; i < length; i++) {
            if (attribute[nodeChildren[nodeId][i]] == childAttribute) {
                return nodeChildren[nodeId][i];
            }
        }
        return -1;
    }

    public long count(final int nodeId) {
        return nodeCount[nodeId];
    }

    public Map<Integer,MutableLong> generateFList() {
        final Map<Integer,MutableLong> frequencyList = Maps.newHashMap();
        final Iterator<ItemsListWithSupport> it = iterator();
        //int items = 0;
        //int count = 0;
        while (it.hasNext()) {
            final ItemsListWithSupport p = it.next();
            final int[] items= p.getKey();
            for (int idx = 0; idx < items.length; idx++) {
                if (!frequencyList.containsKey(items[idx])) {
                    frequencyList.put(items[idx], new MutableLong(0));
                }
                frequencyList.get(items[idx]).add(p.getValue());
            }
        }
        return frequencyList;
    }

    public TransactionTree getCompressedTree() {
        final TransactionTree ctree = new TransactionTree();
        final Iterator<ItemsListWithSupport> it = iterator();
        int node = 0;
        int size = 0;
        final List<ItemsListWithSupport> compressedTransactionSet = Lists.newArrayList();
        while (it.hasNext()) {
            final ItemsListWithSupport p = it.next();
            int[] key = p.getKey();
            Arrays.sort(key);
            compressedTransactionSet.add(p);
            node += ctree.addPattern(key, p.getValue());
            size += key.length + 2;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Nodes in UnCompressed Tree: {} ", nodes);
            LOG.debug("UnCompressed Tree Size: {}", (this.nodes * 4 * 4 + this.childCount() * 4) / 1000000.0);
            LOG.debug("Nodes in Compressed Tree: {} ", node);
            LOG.debug("Compressed Tree Size: {}", (node * 4 * 4 + ctree.childCount() * 4) / 1000000.0);
            LOG.debug("TransactionSet Size: {}", size * 4 / 1000000.0);
        }
        if (node * 4 * 4 + ctree.childCount() * 4 <= size * 4) {
            return ctree;
        } else {
            return new TransactionTree(compressedTransactionSet);
        }
    }

    @Override
    public Iterator<ItemsListWithSupport> iterator() {
        if (this.isTreeEmpty() && !representedAsList) {
            throw new IllegalStateException("This is a bug. Please report this to mahout-user list");
        } else if (representedAsList) {
            return transactionSet.iterator();
        } else {
            return new TransactionTreeIterator(this);
        }
    }

    public boolean isTreeEmpty() {
        return nodes <= 1;
    }

    private int createNode(final int parentNodeId, final int attributeValue, final long count) {
        if (nodes >= this.attribute.length) {
            resize();
        }

        childCount[nodes] = 0;
        this.attribute[nodes] = attributeValue;
        nodeCount[nodes] = count;
        if (nodeChildren[nodes] == null) {
            nodeChildren[nodes] = new int[DEFAULT_CHILDREN_INITIAL_SIZE];
        }

        final int childNodeId = nodes++;
        addChild(parentNodeId, childNodeId);
        return childNodeId;
    }

    private int createRootNode() {
        childCount[nodes] = 0;
        attribute[nodes] = -1;
        nodeCount[nodes] = 0;
        if (nodeChildren[nodes] == null) {
            nodeChildren[nodes] = new int[DEFAULT_CHILDREN_INITIAL_SIZE];
        }
        return nodes++;
    }

    private void resize() {
        int size = (int) (GROWTH_RATE * nodes);
        if (size < DEFAULT_INITIAL_SIZE) {
            size = DEFAULT_INITIAL_SIZE;
        }

        final int[] oldChildCount = childCount;
        final int[] oldAttribute = attribute;
        final long[] oldnodeCount = nodeCount;
        final int[][] oldNodeChildren = nodeChildren;

        childCount = new int[size];
        attribute = new int[size];
        nodeCount = new long[size];
        nodeChildren = new int[size][];

        System.arraycopy(oldChildCount, 0, this.childCount, 0, nodes);
        System.arraycopy(oldAttribute, 0, this.attribute, 0, nodes);
        System.arraycopy(oldnodeCount, 0, this.nodeCount, 0, nodes);
        System.arraycopy(oldNodeChildren, 0, this.nodeChildren, 0, nodes);
    }

    private void resizeChildren(final int nodeId) {
        final int length = childCount[nodeId];
        int size = (int) (GROWTH_RATE * length);
        if (size < DEFAULT_CHILDREN_INITIAL_SIZE) {
            size = DEFAULT_CHILDREN_INITIAL_SIZE;
        }
        final int[] oldNodeChildren = nodeChildren[nodeId];
        nodeChildren[nodeId] = new int[size];
        System.arraycopy(oldNodeChildren, 0, this.nodeChildren[nodeId], 0, length);
    }

    @Override
    public String toString() {
        return "TransactionTree{" +
                "attribute=" + Arrays.toString(attribute) +
                ", childCount=" + Arrays.toString(childCount) +
                ", nodeChildren=" + Arrays.toString(nodeChildren) +
                ", nodeCount=" + Arrays.toString(nodeCount) +
                ", nodes=" + nodes +
                ", representedAsList=" + representedAsList +
                ", transactionSet=" + transactionSet +
                '}';
    }
}
