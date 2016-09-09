package com.stewel.dataflow.fpgrowth;

import java.util.Comparator;

/**
 * A Comparator for comparing two itemsets having the same size using the lexical order.
 */
public class ItemsetComparator implements Comparator<int[]> {

    /**
     * Compare two itemsets and return -1,0 and 1 if the second itemset is larger, equal or smaller than the first
     * itemset according to the lexical order.
     */
    @Override
    public int compare(final int[] itemset1, final int[] itemset2) {
        // for each item in the first itemset
        for (int i = 0; i < itemset1.length; i++) {
            // if the current item is smaller in the first itemset
            if (itemset1[i] < itemset2[i]) {
                return -1; // than the first itemset is smaller
                // if the current item is larger in the first itemset
            } else if (itemset2[i] < itemset1[i]) {
                return 1; // than the first itemset is larger
            }
            // otherwise they are equal so the next item in both itemsets will be compared next.
        }
        return 0; // both itemsets are equal
    }

}
