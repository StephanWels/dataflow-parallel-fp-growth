package com.stewel.dataflow.assocrules;

import java.util.ArrayList;
import java.util.List;

public class ItemsetsCandidateGenerator {

    /**
     * Generating candidate itemsets of size k from frequent itemsets of size k-1. This is called "apriori-gen" in the
     * paper by agrawal.
     *
     * Note that this method is very optimized. It assumed that the list of itemsets received as parameter are lexically
     * ordered.
     *
     * @param levelK_1 a set of itemsets of size k-1
     * @return a set of candidates
     */
    protected List<int[]> generateCandidateSizeK(final List<int[]> levelK_1) {
        // create a variable to store candidates
        final List<int[]> candidates = new ArrayList<int[]>();
        // For each itemset I1 and I2 of level k-1
        loop1:
        for (int i = 0; i < levelK_1.size(); i++) {
            final int[] itemset1 = levelK_1.get(i);
            loop2:
            for (int j = i + 1; j < levelK_1.size(); j++) {
                final int[] itemset2 = levelK_1.get(j);
                // We compare items of itemset1 and itemset2. If they have all the same k-1 items and the last item of
                // itemset1 is smaller than the last item of itemset2, we will combine them to generate a candidate.
                for (int k = 0; k < itemset1.length; k++) {
                    // if they are the last items
                    if (k == itemset1.length - 1) {
                        // the one from itemset1 should be smaller (lexical order)
                        // and different from the one of itemset2
                        if (itemset1[k] >= itemset2[k]) {
                            continue loop1;
                        }
                    }
                    // if they are not the last items, and
                    else if (itemset1[k] < itemset2[k]) {
                        continue loop2; // we continue searching
                    } else if (itemset1[k] > itemset2[k]) {
                        continue loop1; // we stop searching: because of lexical order
                    }
                }
                // Create a new candidate by combining itemset1 and itemset2
                final int lastItem1 = itemset1[itemset1.length - 1];
                final int lastItem2 = itemset2[itemset2.length - 1];
                final int newItemset[];
                if (lastItem1 < lastItem2) {
                    // Create a new candidate by combining itemset1 and itemset2
                    newItemset = new int[itemset1.length + 1];
                    System.arraycopy(itemset1, 0, newItemset, 0, itemset1.length);
                    newItemset[itemset1.length] = lastItem2;
                    candidates.add(newItemset);
                } else {
                    // Create a new candidate by combining itemset1 and itemset2
                    newItemset = new int[itemset1.length + 1];
                    System.arraycopy(itemset2, 0, newItemset, 0, itemset2.length);
                    newItemset[itemset2.length] = lastItem1;
                    candidates.add(newItemset);
                }

            }
        }
        return candidates;
    }

}
