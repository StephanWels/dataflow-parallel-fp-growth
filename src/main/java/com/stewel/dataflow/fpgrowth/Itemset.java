package com.stewel.dataflow.fpgrowth;

import java.util.ArrayList;

public class Itemset {
    private final int[] itemset;
    private int absoluteSupport;

    public Itemset(int[] itemset) {
        this.itemset = itemset;
    }

    public Itemset(ArrayList<Integer> items, Long support) {
        itemset = new int[items.size()];
        for (int i = 0; i < items.size(); i++) {
            itemset[i] = items.get(i);
        }
        absoluteSupport = support.intValue();
    }

    public void setAbsoluteSupport(int absoluteSupport) {
        this.absoluteSupport = absoluteSupport;
    }

    public int size() {
        return itemset.length;
    }

    public int getAbsoluteSupport() {
        return absoluteSupport;
    }

    public int[] getItems() {
        return itemset;
    }
}
