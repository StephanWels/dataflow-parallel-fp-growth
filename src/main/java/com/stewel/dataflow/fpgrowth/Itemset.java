package com.stewel.dataflow.fpgrowth;

import org.immutables.value.Value;

import java.util.Arrays;

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

    /**
     * Make a copy of this itemset but exclude a given item.
     *
     * @param itemToRemove the item that should be excluded
     * @return the copy of this itemset except the item that should be excluded
     */
    public static int[] cloneItemSetMinusOneItem(final int[] itemset, final int itemToRemove) {
        final int[] newItemset = new int[itemset.length - 1];
        int i = 0;
        for (final int anItemset : itemset) {
            // copy the item except if it is the item that should be excluded
            if (anItemset != itemToRemove) {
                newItemset[i++] = anItemset;
            }
        }
        return newItemset;
    }

    /**
     * Make a copy of this itemset but exclude a set of items
     *
     * @param itemsetToNotKeep the set of items to be excluded
     * @return the copy of this itemset except the set of items to be excluded
     */
    public static int[] cloneItemSetMinusAnItemset(final int[] itemset, final int[] itemsetToNotKeep) {
        final int[] newItemset = new int[itemset.length - itemsetToNotKeep.length];
        int i = 0;
        for (final int anItemset : itemset) {
            // copy the item except if it is not an item that should be excluded
            if (Arrays.binarySearch(itemsetToNotKeep, anItemset) < 0) {
                newItemset[i++] = anItemset;
            }
        }
        return newItemset;
    }
}
