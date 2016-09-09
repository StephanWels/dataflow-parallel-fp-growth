package com.stewel.dataflow.fpgrowth;

import org.immutables.value.Value;

/**
 * This class represents an itemset (a set of items) implemented as an array of integers with
 * a variable to store the support count of the itemset.
 */
@Value.Immutable
public abstract class Itemset {


    public abstract int[] getItems();

    public abstract long getAbsoluteSupport();

    public int size() {
        return getItems().length;
    }
}
