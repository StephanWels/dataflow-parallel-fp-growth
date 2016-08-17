package com.stewel.dataflow.fpgrowth;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.Lists;
import com.stewel.dataflow.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A straightforward implementation of FPTrees as described in Han et. al.
 */
public class FPTree {
    private static final Logger log = LoggerFactory.getLogger(FPTree.class);

    private final AttrComparator attrComparator = new AttrComparator();
    private FPNode root;
    private long minSupport;
    private ArrayList<Long> attrCountList;
    private Map<Integer, Object> attrNodeLists;

    public class FPNode {
        private FPNode parent;
        private Map<Integer, FPNode> childMap;
        private int attribute;
        private long count;

        private FPNode(FPNode parent, int attribute, long count) {
            this.parent = parent;
            this.attribute = attribute;
            this.count = count;
            this.childMap = new HashMap<>();
        }

        private void addChild(FPNode child) {
            this.childMap.put(child.attribute(), child);
        }

        public Iterable<FPNode> children() {
            return childMap.values();
        }

        public int numChildren() {
            return childMap.size();
        }

        public FPNode parent() {
            return parent;
        }

        public FPNode child(int attribute) {
            return childMap.get(attribute);
        }

        public int attribute() {
            return attribute;
        }

        public void accumulate(long incr) {
            count = count + incr;
        }

        public long count() {
            return count;
        }

    }

    /**
     * Creates an FPTree using the attribute counts in attrCountList.
     * <p>
     * Note that the counts in attrCountList are assumed to be complete;
     * they are not updated as the tree is modified.
     */
    public FPTree(ArrayList<Long> attrCountList, long minSupport) {
        this.root = new FPNode(null, -1, 0);
        this.attrCountList = attrCountList;
        this.attrNodeLists = new HashMap<>();
        this.minSupport = minSupport;
    }

    /**
     * Creates an FPTree using the attribute counts in attrCounts.
     * <p>
     * Note that the counts in attrCounts are assumed to be complete;
     * they are not updated as the tree is modified.
     */
    public FPTree(long[] attrCounts, long minSupport) {
        this.root = new FPNode(null, -1, 0);
        this.attrCountList = new ArrayList<>();
        for (int i = 0; i < attrCounts.length; i++)
            if (attrCounts[i] > 0) {
                if (attrCountList.size() < (i + 1)) {
                    attrCountList.add(attrCounts[i]);
                } else {
                    attrCountList.set(i, attrCounts[i]);
                }
            }
        this.attrNodeLists = new HashMap<>();
        this.minSupport = minSupport;
    }


    /**
     * Returns the count of the given attribute, as supplied on construction.
     */
    public long headerCount(int attribute) {
        return attrCountList.get(attribute);
    }

    /**
     * Returns the root node of the tree.
     */
    public FPNode root() {
        return root;
    }

    /**
     * Adds an itemset with the given occurrance count.
     */
    public void accumulate(ArrayList<Integer> argItems, long count) {
        // boxed primitive used so we can use custom comparitor in sort
        List<Integer> items = Lists.newArrayList();
        for (int i = 0; i < argItems.size(); i++) {
            items.add(argItems.get(i));
        }
        Collections.sort(items, attrComparator);

        FPNode currNode = root;
        for (int i = 0; i < items.size(); i++) {
            int item = items.get(i);
            long attrCount = 0;
            if (item < attrCountList.size())
                attrCount = attrCountList.get(item);
            if (attrCount < minSupport)
                continue;

            FPNode next = currNode.child(item);
            if (next == null) {
                next = new FPNode(currNode, item, count);
                currNode.addChild(next);
                List<FPNode> nodeList = (List<FPNode>) attrNodeLists.get(item);
                if (nodeList == null) {
                    nodeList = Lists.newArrayList();
                    attrNodeLists.put(item, nodeList);
                }
                nodeList.add(next);
            } else {
                next.accumulate(count);
            }
            currNode = next;
        }
    }

    /**
     * Adds an itemset with the given occurrance count.
     */
    public void accumulate(List<Integer> argItems, long count) {
        List<Integer> items = Lists.newArrayList();
        items.addAll(argItems);
        Collections.sort(items, attrComparator);

        FPNode currNode = root;
        for (int i = 0; i < items.size(); i++) {
            int item = items.get(i);
            long attrCount = attrCountList.get(item);
            if (attrCount < minSupport)
                continue;

            FPNode next = currNode.child(item);
            if (next == null) {
                next = new FPNode(currNode, item, count);
                currNode.addChild(next);
                List<FPNode> nodeList = (List<FPNode>) attrNodeLists.get(item);
                if (nodeList == null) {
                    nodeList = Lists.newArrayList();
                    attrNodeLists.put(item, nodeList);
                }
                nodeList.add(next);
            } else {
                next.accumulate(count);
            }
            currNode = next;
        }
    }


    /**
     * Returns an Iterable over the attributes in the tree, sorted by
     * frequency (low to high).
     */
    public Iterable<Integer> attrIterable() {
        List<Integer> attrs = Lists.newArrayList();
        for (int i = 0; i < attrCountList.size(); i++) {
            if (attrCountList.get(i) > 0)
                attrs.add(i);
        }
        Collections.sort(attrs, attrComparator);
        return attrs;
    }

    /**
     * Returns an Iterable over the attributes in the tree, sorted by
     * frequency (high to low).
     */
    public Iterable<Integer> attrIterableRev() {
        List<Integer> attrs = Lists.newArrayList();
        for (int i = 0; i < attrCountList.size(); i++) {
            if (attrCountList.get(i) > 0)
                attrs.add(i);
        }
        Collections.sort(attrs, Collections.reverseOrder(attrComparator));
        return attrs;
    }

    /**
     * Returns a conditional FP tree based on the targetAttr, containing
     * only items that are more frequent.
     */
    public FPTree createMoreFreqConditionalTree(int targetAttr) {
        ArrayList<Long> counts = new ArrayList<Long>();
        List<FPNode> nodeList = (List<FPNode>) attrNodeLists.get(targetAttr);

        for (FPNode currNode : nodeList) {
            long pathCount = currNode.count();
            while (currNode != root) {
                int currAttr = currNode.attribute();
                long count = counts.get(currAttr);
                if (counts.size() <= currAttr) {
                    counts.add(count + pathCount);
                } else {
                    counts.set(currNode.attribute(), count + pathCount);
                }
                currNode = currNode.parent();
            }
        }
        if (counts.get(targetAttr) != attrCountList.get(targetAttr))
            throw new IllegalStateException("mismatched counts for targetAttr="
                    + targetAttr + ", (" + counts.get(targetAttr)
                    + " != " + attrCountList.get(targetAttr) + "); "
                    + "thisTree=" + this + "\n");
        counts.set(targetAttr, 0L);

        FPTree toRet = new FPTree(counts, minSupport);
        ArrayList<Integer> attrLst = new ArrayList<>();
        for (FPNode currNode : (List<FPNode>) attrNodeLists.get(targetAttr)) {
            long count = currNode.count();
            attrLst.clear();
            while (currNode != root) {
                if (currNode.count() < count)
                    throw new IllegalStateException();
                attrLst.add(currNode.attribute());
                currNode = currNode.parent();
            }

            toRet.accumulate(attrLst, count);
        }
        return toRet;
    }

    // biggest count or smallest attr number goes first
    private class AttrComparator implements Comparator<Integer> {
        public int compare(Integer a, Integer b) {

            long aCnt = 0;
            if (a < attrCountList.size())
                aCnt = attrCountList.get(a);
            long bCnt = 0;
            if (b < attrCountList.size())
                bCnt = attrCountList.get(b);
            if (aCnt == bCnt)
                return a - b;
            return (bCnt - aCnt) < 0 ? -1 : 1;
        }
    }

    /**
     * Return a pair of trees that result from separating a common prefix
     * (if one exists) from the lower portion of this tree.
     */
    public Pair<FPTree, FPTree> splitSinglePrefix() {
        if (root.numChildren() != 1) {
            return new Pair<>(null, this);
        }
        ArrayList<Long> pAttrCountList = new ArrayList<>();
        ArrayList<Long> qAttrCountList = new ArrayList<>(attrCountList);

        FPNode currNode = root;
        while (currNode.numChildren() == 1) {
            currNode = currNode.children().iterator().next();
            if (pAttrCountList.size() <= currNode.attribute()) {
                pAttrCountList.add(currNode.count());
            } else {
                pAttrCountList.set(currNode.attribute(), currNode.count());
            }
            qAttrCountList.set(currNode.attribute(), 0L);
        }

        FPTree pTree = new FPTree(pAttrCountList, minSupport);
        FPTree qTree = new FPTree(qAttrCountList, minSupport);
        recursivelyAddPrefixPats(pTree, qTree, root, null);

        return new Pair<>(pTree, qTree);
    }

    private long recursivelyAddPrefixPats(FPTree pTree, FPTree qTree, FPNode node,
                                          ArrayList<Integer> items) {
        long added = 0;
        long count = node.count();
        int attribute = node.attribute();
        if (items == null) {
            // at root
            if (!(node == root))
                throw new IllegalStateException();
            items = new ArrayList<>();
        } else {
            items.add(attribute);
        }
        for (FPNode child : node.children()) {
            added += recursivelyAddPrefixPats(pTree, qTree, child, items);
        }
        if (added < count) {
            long toAdd = count - added;
            pTree.accumulate(items, toAdd);
            qTree.accumulate(items, toAdd);
            added += toAdd;
        }
        if (!(node == root)) {
            int lastIdx = items.size() - 1;
            if (items.get(lastIdx) != attribute) {
                throw new IllegalStateException();
            }
            items.remove(lastIdx);
        }
        return added;
    }

    private void toStringHelper(StringBuilder sb, FPNode currNode, String prefix) {
        if (currNode.numChildren() == 0) {
            sb.append(prefix).append("-{attr:").append(currNode.attribute())
                    .append(", cnt:").append(currNode.count()).append("}\n");
        } else {
            StringBuilder newPre = new StringBuilder(prefix);
            newPre.append("-{attr:").append(currNode.attribute())
                    .append(", cnt:").append(currNode.count()).append('}');
            StringBuilder fakePre = new StringBuilder();
            while (fakePre.length() < newPre.length()) {
                fakePre.append(' ');
            }
            int i = 0;
            for (FPNode child : currNode.children())
                toStringHelper(sb, child, (i++ == 0 ? newPre : fakePre).toString() + '-' + i + "->");
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("[FPTree\n");
        toStringHelper(sb, root, "  ");
        sb.append("]");
        return sb.toString();
    }

}