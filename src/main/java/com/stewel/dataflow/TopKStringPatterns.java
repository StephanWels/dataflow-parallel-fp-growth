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
                    cmp = myItem.getKey().size() - otherItem.getKey().size();
                    if (cmp == 0) {
                        for (int j = 0; j < myItem.getKey().size(); j++) {
                            cmp = myItem.getKey().get(j).compareTo(
                                    otherItem.getKey().get(j));
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
