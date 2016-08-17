package com.stewel.dataflow.fpgrowth;

public class Itemset {
    private final int[] itemset;
    private int absoluteSupport;

    public Itemset(int[] itemset) {

        this.itemset = itemset;
    }

    public void setAbsoluteSupport(int absoluteSupport) {
        this.absoluteSupport = absoluteSupport;
    }

    public int size() {
        return itemset.length;
    }
}
