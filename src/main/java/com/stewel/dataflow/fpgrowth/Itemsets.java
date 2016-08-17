package com.stewel.dataflow.fpgrowth;

import java.util.ArrayList;
import java.util.List;

public class Itemsets {
    /**
     * We store the itemsets in a list named "levels".
     * Position i in "levels" contains the list of itemsets of size i
     */
    private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>();
    private final String itemsetsName;
    private int itemsetsCount;

    public Itemsets(String itemsetsName) {
        this.itemsetsName = itemsetsName;
    }

    public void addItemset(Itemset itemset, int k) {
        while (levels.size() <= k) {
            levels.add(new ArrayList<Itemset>());
        }
        levels.get(k).add(itemset);
        itemsetsCount++;
    }
}
