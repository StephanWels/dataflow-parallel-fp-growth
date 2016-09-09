package com.stewel.dataflow.fpgrowth;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a set of itemsets where an itemset is an array of integers. Itemsets are ordered by size. For
 * example, level 1 means itemsets of size 1 (that contains 1 item).
 *
 * @author Philippe Fournier-Viger
 */
public class Itemsets {

    /**
     * A name that we give to these itemsets (e.g. "frequent itemsets").
     */
    private final String name;

    /**
     * We store the itemsets in a list named "levels". Position i in "levels" contains the list of itemsets of size i.
     */
    private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>();

    /**
     * The total number of itemsets.
     */
    private int itemsetsCount = 0;

    public Itemsets(final String name) {
        this.name = name;
        levels.add(new ArrayList<Itemset>()); // We create an empty level 0 by default.
    }

    public String getName() {
        return name;
    }

    /**
     * Add an itemset to this structure.
     *
     * @param itemset the itemset
     */
    public void addItemset(final Itemset itemset) {
        final int level = itemset.size();
        while (levels.size() <= level) {
            levels.add(new ArrayList<>());
        }
        levels.get(level).add(itemset);
        itemsetsCount++;
    }

    /**
     * Get all itemsets.
     *
     * @return A list of list of itemsets.
     * Position i in this list is the list of itemsets of size i.
     */
    public List<List<Itemset>> getLevels() {
        return levels;
    }
}