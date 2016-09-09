package com.stewel.dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public final class TopKStringPatterns implements Serializable {
    
    private final List<ItemsListWithSupport> frequentPatterns;

    public TopKStringPatterns() {
        frequentPatterns = Lists.newArrayList();
    }

    public TopKStringPatterns(Collection<ItemsListWithSupport> patterns) {
        frequentPatterns = Lists.newArrayList();
        frequentPatterns.addAll(patterns);
    }

    public Iterator<ItemsListWithSupport> iterator() {
        return frequentPatterns.iterator();
    }

    public List<ItemsListWithSupport> getPatterns() {
        return frequentPatterns;
    }

    public TopKStringPatterns merge(TopKStringPatterns pattern, int heapSize) {
        List<ItemsListWithSupport> patterns = Lists.newArrayList();
        Iterator<ItemsListWithSupport> myIterator = frequentPatterns.iterator();
        Iterator<ItemsListWithSupport> otherIterator = pattern.iterator();
        ItemsListWithSupport myItem = null;
        ItemsListWithSupport otherItem = null;
        for (int i = 0; i < heapSize; i++) {
            if (myItem == null && myIterator.hasNext()) {
                myItem = myIterator.next();
            }
            if (otherItem == null && otherIterator.hasNext()) {
                otherItem = otherIterator.next();
            }
            if (myItem != null && otherItem != null) {
                int cmp = myItem.getValue().compareTo(otherItem.getValue());
                if (cmp == 0) {
                    int[] key = myItem.getKey();
                    int[] otherKey = otherItem.getKey();
                    cmp = key.length - otherKey.length;
                    if (cmp == 0) {
                        for (int j = 0; j < key.length; j++) {
                            cmp = compare(key[j], otherKey[j]);
                            if (cmp != 0) {
                                break;
                            }
                        }
                    }
                }
                if (cmp <= 0) {
                    patterns.add(otherItem);
                    if (cmp == 0) {
                        myItem = null;
                    }
                    otherItem = null;
                } else if (cmp > 0) {
                    patterns.add(myItem);
                    myItem = null;
                }
            } else if (myItem != null) {
                patterns.add(myItem);
                myItem = null;
            } else if (otherItem != null) {
                patterns.add(otherItem);
                otherItem = null;
            } else {
                break;
            }
        }
        return new TopKStringPatterns(patterns);
    }

    public static int compare(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (ItemsListWithSupport pattern : frequentPatterns) {
            sb.append(sep);
            sb.append(pattern.toString());
            sep = ", ";

        }
        return sb.toString();

    }
}
