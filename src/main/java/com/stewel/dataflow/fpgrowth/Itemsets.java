package com.stewel.dataflow.fpgrowth;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a set of itemsets where an itemset is an array of integers. Itemsets are ordered by size. For
 * example, level 1 means itemsets of size 1 (that contains 1 item).
 */
public class Itemsets {
    /**
     * We store the itemsets in a list named "levels". Position i in "levels" contains the list of itemsets of size i.
     */
    private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>();

    public Itemsets() {
        levels.add(new ArrayList<Itemset>()); // We create an empty level 0 by default.
    }

    /**
     * Add an itemset to this structure.
     *
     * @param itemset the itemset
     */
    public void addItemset(final Itemset itemset) {
        final int levelNumber = itemset.size();
        while (levels.size() <= levelNumber) {
            levels.add(new ArrayList<>());
        }
        levels.get(levelNumber).add(itemset);
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

    @Override
    public final boolean equals(final Object object) {
        return EqualsBuilder.reflectionEquals(this, object);
    }

    @Override
    public final int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public final String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}